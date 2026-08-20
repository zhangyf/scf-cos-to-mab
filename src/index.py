# -*- coding: utf-8 -*-
"""
SCF 云函数：把普通 COS 桶某前缀下新上传的对象搬运到同地域「元数据加速桶」(MAB)。

地域：ap-tokyo（腾讯云国际站）
映射：srcbucket/prod-logs/2026/08/20/xxx  ->  dstmab/prod-raw-logs/2026/08/20/xxx
      （剥掉 SRC_PREFIX，其下层级原样保留，拼上 DST_PREFIX）

【为什么不用 CopyObject】
官方文档明确：启用元数据加速能力的存储桶暂不支持以下接口
    PUT Object - Copy / Upload Part - Copy / DELETE Object
    https://cloud.tencent.com/document/product/436/73685 「注意事项」
所以本函数只能 GetObject 取字节流再 PutObject 写入，不能用 copy_object / upload_part_copy。

【MAB 其它相关限制】https://cloud.tencent.com/document/product/436/56971
  - 不支持自定义头部 x-cos-meta-*（本场景不需要保留自定义元数据）
  - 存储桶复制 / COS Batch / 对象标签 / 版本控制 均不适用
  - list 时 prefix 仅支持目录且不向下递归（本函数不做任何 list，故不受影响）
  - PutObject 若 key 含中间路径，会自动递归创建中间子目录，无需预建目录

【失败处理】
不配对账函数、不配死信队列。所有结果一律打单行 JSON 日志，靠 CLS 检索 + 告警兜底。
result 取值只有五种：OK / SKIP_EXISTS / SKIP_DIR / RETRY / FAILED
  - RETRY ：可重试错误，raise 交由 SCF 异步重试（默认 2 次，间隔 1 分钟）
  - FAILED：不可重试错误，直接返回，不浪费平台重试次数
失败日志带齐 srcKey/dstKey/size/错误码，可直接捞出来人工重放。
"""

import json
import os
import time
from urllib.parse import unquote, unquote_plus

from qcloud_cos import CosConfig, CosS3Client
from qcloud_cos.cos_exception import CosServiceError, CosClientError

# SDK 默认会打大量 INFO 日志，量大时会淹没自己的结构化日志
import logging
logging.getLogger('qcloud_cos').setLevel(logging.WARNING)


# ============================ 配置 ============================

def _env_bool(name, default='false'):
    return os.environ.get(name, default).strip().lower() in ('true', '1', 'yes')


def _env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return int(default)


REGION = os.environ.get('COS_REGION', 'ap-tokyo').strip()
SRC_BUCKET = os.environ.get('SRC_BUCKET', '').strip()
DST_BUCKET = os.environ.get('DST_BUCKET', '').strip()

# 前缀必须以 / 结尾、且不以 / 开头（COS 对象键本身不以 / 开头）
def _norm_prefix(raw, default):
    p = (raw or default).strip().lstrip('/')
    if p and not p.endswith('/'):
        p += '/'
    return p


SRC_PREFIX = _norm_prefix(os.environ.get('SRC_PREFIX'), 'prod-logs/')
DST_PREFIX = _norm_prefix(os.environ.get('DST_PREFIX'), 'prod-raw-logs/')

SMALL_MB = _env_int('SMALL_MB', 100)          # <= 该值全量入内存直传
PART_MB = _env_int('PART_MB', 16)             # 分块上传的分块大小
MAX_OBJECT_GB = _env_int('MAX_OBJECT_GB', 48)  # 硬上限，超过直接判 FAILED
IDEMPOTENT = _env_bool('IDEMPOTENT', 'true')   # 搬运前 Head 目标做幂等校验
VERIFY_CRC = _env_bool('VERIFY_CRC', 'true')   # Put 后比对 crc64
USE_INTERNAL = _env_bool('USE_INTERNAL_DOMAIN', 'false')
DST_USE_INTERNAL = _env_bool('DST_USE_INTERNAL_DOMAIN', 'false')

# key 解码方式。COS event 里的 key 是 URL 编码的，但官方文档没说明用哪种解码。
#   unquote_plus：会把 '+' 解成空格。若源文件名本身含 '+'（如 a+b.log），会被改错。
#   unquote     ：保留 '+' 原样。
# 默认 unquote_plus（兼容 key 里真的用 '+' 代表空格的情况）。
# 若你的日志文件名含 '+'，把 DECODE_PLUS 设为 false。verify.py 会实测这一项。
DECODE_PLUS = _env_bool('DECODE_PLUS', 'true')

SMALL_BYTES = SMALL_MB * 1024 * 1024
PART_BYTES = max(PART_MB, 1) * 1024 * 1024
MAX_BYTES = MAX_OBJECT_GB * 1024 * 1024 * 1024

CRC_HEADER = 'x-cos-hash-crc64ecma'

# 这些错误码重试也不会成功，直接判 FAILED，不占用平台重试次数
NON_RETRYABLE_CODES = frozenset([
    'AccessDenied', 'NoSuchKey', 'NoSuchResource', 'NoSuchBucket',
    'InvalidArgument', 'InvalidObjectName', 'KeyTooLong', 'EntityTooLarge',
    'SignatureDoesNotMatch', 'InvalidDigest', 'InvalidRequest',
    'MethodNotAllowed', 'NotImplemented', 'UnsupportedOperation',
])


# ============================ 异常 ============================

class NonRetryable(Exception):
    """业务性错误，重试无意义 -> FAILED"""


class SkipObject(Exception):
    """无需搬运（如目录占位对象）-> SKIP_DIR，不算失败"""


class ShortRead(Exception):
    """读到的字节数与预期不符，可能是网络截断 -> RETRY"""


# ============================ 客户端 ============================

def _internal_domain(bucket):
    # 同地域内网域名格式：<bucket>.cos-internal.<region>.tencentcos.cn
    # MAB 是否支持该域名官方文档未提及，故做成开关，默认关闭走公网域名
    return '{0}.cos-internal.{1}.tencentcos.cn'.format(bucket, REGION)


def _build_client(bucket, use_internal):
    """
    密钥从自定义环境变量读：

        COS_SECRET_ID    = AKIDxxxx
        COS_SECRET_KEY   = xxxx
        COS_SESSION_TOKEN = xxxx   （可选，仅使用临时密钥时需要）

    刻意不用 TENCENTCLOUD_ / QCLOUD_ / SCF_ 前缀 —— SCF 保留了这些前缀，
    自定义环境变量不允许使用。

    若配置了运行角色而想改用平台注入的临时密钥，把上面三个变量的值
    分别指向平台注入值即可，代码无需改动。

    Token 只在有值时才传：用长期密钥时该变量为空，
    传 None 可能导致 SDK 在签名里带上空的 security-token 头。
    """
    kwargs = {
        'Region': REGION,
        'SecretId': os.environ.get('COS_SECRET_ID'),
        'SecretKey': os.environ.get('COS_SECRET_KEY'),
        'Scheme': 'https',
    }
    token = os.environ.get('COS_SESSION_TOKEN')
    if token:
        kwargs['Token'] = token
    if use_internal:
        kwargs['Domain'] = _internal_domain(bucket)
        kwargs['Scheme'] = 'http'   # 内网链路省掉 TLS 握手开销
    return CosS3Client(CosConfig(**kwargs))


# 模块级创建，容器热启动时复用，避免每次调用重建连接池
SRC_CLIENT = _build_client(SRC_BUCKET, USE_INTERNAL)
DST_CLIENT = _build_client(DST_BUCKET, DST_USE_INTERNAL)


# ============================ 日志 ============================

def log(**fields):
    """单行 JSON 日志。字段固定，便于 CLS 按 result 精确检索与告警。"""
    fields.setdefault('ts', int(time.time()))
    try:
        print(json.dumps(fields, ensure_ascii=False, sort_keys=True))
    except (TypeError, ValueError):
        print(json.dumps({'level': 'ERROR', 'msg': 'log_serialize_failed',
                          'raw': repr(fields)[:2000]}))


# ============================ 事件解析 ============================

def _decode_key(raw):
    """COS event 里的 key 是 URL 编码的，解码方式由 DECODE_PLUS 控制（见配置段注释）。"""
    return unquote_plus(raw) if DECODE_PLUS else unquote(raw)


def parse_event(event):
    """
    支持两种入参：

    1) COS 触发器事件（正常链路）
       https://cloud.tencent.com/document/product/583/9707
       Records[i].cos.cosObject.key 形如 "/1250000000/srcbucket/prod-logs/2026/08/20/a.gz"
       -> 必须剥掉 "/<appid>/<bucketname>/" 才是真正的对象键
       cosBucket.region 是简称（如 "cd" 而非 "ap-chengdu"），不可用，一律用环境变量

    2) 手动重放：{"keys": ["prod-logs/2026/08/20/a.gz", ...]}
       从失败日志里捞出 srcKey 列表后可直接 invoke，不必手工拼完整 event 结构
    """
    if isinstance(event, dict) and event.get('keys'):
        out = []
        for k in event['keys']:
            key = str(k).lstrip('/')
            if key.startswith(SRC_BUCKET + '/'):     # 容忍误粘 bucket 前缀
                key = key[len(SRC_BUCKET) + 1:]
            out.append({'key': key, 'size': None,
                        'eventName': 'manual:Replay', 'cosReqId': ''})
        return out

    records = []
    if isinstance(event, dict):
        records = event.get('Records') or []

    out = []
    for rec in records:
        cos = (rec or {}).get('cos') or {}
        obj = cos.get('cosObject') or {}
        bkt = cos.get('cosBucket') or {}

        key = _decode_key(str(obj.get('key', '')))    # key 是 URL 编码的
        key = key.lstrip('/')

        # 逐段剥离 appid 和 bucketname
        for seg in (str(bkt.get('appid', '')), str(bkt.get('name', ''))):
            if seg and key.startswith(seg + '/'):
                key = key[len(seg) + 1:]

        size = obj.get('size')
        out.append({
            'key': key,
            'size': int(size) if isinstance(size, int) else None,
            'eventName': ((rec or {}).get('event') or {}).get('eventName', ''),
            'cosReqId': (obj.get('meta') or {}).get('x-cos-request-id', ''),
        })
    return out


def map_key(src_key):
    """
    纯前缀替换，SRC_PREFIX 之后的层级原样保留：
        prod-logs/2026/08/20/nginx/a.log -> prod-raw-logs/2026/08/20/nginx/a.log
    刻意不做任何层级解析或重分区，源路径出现意外层级也不会搬错位置。
    """
    if SRC_PREFIX and not src_key.startswith(SRC_PREFIX):
        raise NonRetryable('key not under SRC_PREFIX({0}): {1}'.format(SRC_PREFIX, src_key))
    rel = src_key[len(SRC_PREFIX):] if SRC_PREFIX else src_key
    if not rel:
        raise SkipObject('empty relative key')
    if rel.endswith('/'):
        # 目录占位对象，MAB 会在 PutObject 时自动建目录，无需也无法搬运
        raise SkipObject('directory placeholder')
    return DST_PREFIX + rel


# ============================ COS 操作 ============================

def head_or_none(client, bucket, key):
    """对象不存在返回 None。SDK 在 404 时抛 CosServiceError(NoSuchResource)。"""
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except CosServiceError as e:
        if e.get_status_code() == 404 or e.get_error_code() in ('NoSuchKey', 'NoSuchResource'):
            return None
        raise


def move_inline(src_key, dst_key, size):
    """
    小文件路径：全量读进内存再整体 Put。
    Content-Length 由 requests 依据 bytes 长度精确推断，不会退化成
    Transfer-Encoding: chunked —— MAB 对 chunked 的支持没有文档保证，必须规避。
    """
    resp = SRC_CLIENT.get_object(Bucket=SRC_BUCKET, Key=src_key)
    data = resp['Body'].get_raw_stream().read()
    if size is not None and len(data) != size:
        raise ShortRead('read {0} bytes, expect {1}'.format(len(data), size))
    return DST_CLIENT.put_object(Bucket=DST_BUCKET, Key=dst_key, Body=data)


def move_multipart(src_key, dst_key, size):
    """
    大文件路径：Range-Get 逐块读 + 分块上传。
    用的是 upload_part（MAB 支持），而不是 upload_part_copy（MAB 明确不支持）。
    每块读完即上传并释放，内存占用恒定为单块大小。
    """
    created = DST_CLIENT.create_multipart_upload(Bucket=DST_BUCKET, Key=dst_key)
    upload_id = created['UploadId']
    parts = []
    try:
        part_no = 1
        offset = 0
        while offset < size:
            end = min(offset + PART_BYTES, size) - 1
            expect = end - offset + 1
            got = SRC_CLIENT.get_object(
                Bucket=SRC_BUCKET, Key=src_key,
                Range='bytes={0}-{1}'.format(offset, end))
            chunk = got['Body'].get_raw_stream().read()
            if len(chunk) != expect:
                raise ShortRead('part {0}: read {1}, expect {2}'.format(
                    part_no, len(chunk), expect))
            uploaded = DST_CLIENT.upload_part(
                Bucket=DST_BUCKET, Key=dst_key, Body=chunk,
                PartNumber=part_no, UploadId=upload_id)
            parts.append({'PartNumber': str(part_no), 'ETag': uploaded['ETag']})
            del chunk                      # 及时释放，控制内存峰值
            offset = end + 1
            part_no += 1
        return DST_CLIENT.complete_multipart_upload(
            Bucket=DST_BUCKET, Key=dst_key, UploadId=upload_id,
            MultipartUpload={'Part': parts})
    except Exception:
        # 失败必须 abort，否则残留的分块会一直计费
        try:
            DST_CLIENT.abort_multipart_upload(
                Bucket=DST_BUCKET, Key=dst_key, UploadId=upload_id)
        except Exception as abort_err:
            log(level='WARN', msg='abort_multipart_failed',
                dstKey=dst_key, uploadId=upload_id, error=str(abort_err))
        raise


# ============================ 单对象处理 ============================

def handle_one(record, request_id):
    """
    返回 (result, item)。result 为 OK / SKIP_EXISTS / SKIP_DIR / FAILED 之一。
    需要平台重试的情况直接向上抛异常，由 main_handler 统一转成 RETRY 日志再 raise。
    """
    started = time.time()
    src_key = record['key']
    item = {
        'requestId': request_id,
        'srcBucket': SRC_BUCKET,
        'srcKey': src_key,
        'dstBucket': DST_BUCKET,
        'eventName': record.get('eventName', ''),
        'cosReqId': record.get('cosReqId', ''),
    }

    dst_key = map_key(src_key)             # SkipObject / NonRetryable 由外层捕获
    item['dstKey'] = dst_key

    # 以源对象的 Head 为准，不信任 event 里的 size（重放场景可能没有）
    src_head = head_or_none(SRC_CLIENT, SRC_BUCKET, src_key)
    if src_head is None:
        raise NonRetryable('source object not found: ' + src_key)

    size = int(src_head.get('Content-Length', 0))
    src_crc = src_head.get(CRC_HEADER)
    item['size'] = size
    item['srcCrc64'] = src_crc

    if size > MAX_BYTES:
        raise NonRetryable('object too large: {0} bytes > {1} GB limit'.format(
            size, MAX_OBJECT_GB))

    # 幂等：SCF 异步会自动重试 2 次，不做幂等就会重复搬同一对象白烧流量。
    # 一次 Head 很便宜，划算。
    if IDEMPOTENT:
        dst_head = head_or_none(DST_CLIENT, DST_BUCKET, dst_key)
        if dst_head is not None and int(dst_head.get('Content-Length', -1)) == size:
            dst_crc = dst_head.get(CRC_HEADER)
            # 双方都能拿到 crc64 时才做严格比对；MAB 是否返回该头待实测，
            # 拿不到就退化为只比 size。
            if not (src_crc and dst_crc) or str(src_crc) == str(dst_crc):
                item['costMs'] = int((time.time() - started) * 1000)
                return 'SKIP_EXISTS', item

    if size <= SMALL_BYTES:
        item['mode'] = 'INLINE'
        put_resp = move_inline(src_key, dst_key, size)
    else:
        item['mode'] = 'MULTIPART'
        put_resp = move_multipart(src_key, dst_key, size)

    dst_crc = (put_resp or {}).get(CRC_HEADER)
    item['dstCrc64'] = dst_crc
    if VERIFY_CRC and src_crc and dst_crc and str(src_crc) != str(dst_crc):
        # MAB 不支持 DELETE Object，脏对象删不掉，只能靠重新 Put 覆盖。
        # 这里不尝试删除，抛错让平台重试（重试会整体重写，覆盖掉脏数据）。
        raise ShortRead('crc64 mismatch: src={0} dst={1}'.format(src_crc, dst_crc))

    item['costMs'] = int((time.time() - started) * 1000)
    return 'OK', item


# ============================ 入口 ============================

def main_handler(event, context):
    request_id = ''
    try:
        request_id = getattr(context, 'request_id', '') or ''
    except Exception:
        request_id = ''
    if not request_id and isinstance(context, dict):
        request_id = context.get('request_id', '') or ''

    # 配置自检：缺项直接给明确报错，避免等到调 COS 时抛看不懂的签名错误
    missing = [n for n, v in (
        ('SRC_BUCKET', SRC_BUCKET),
        ('DST_BUCKET', DST_BUCKET),
        ('COS_SECRET_ID', os.environ.get('COS_SECRET_ID')),
        ('COS_SECRET_KEY', os.environ.get('COS_SECRET_KEY')),
    ) if not v]
    if missing:
        log(level='FATAL', result='FAILED', requestId=request_id,
            error='missing environment variables: ' + ', '.join(missing))
        return {'ok': 0, 'failed': 1, 'results': []}

    records = parse_event(event)
    if not records:
        log(level='WARN', result='SKIP_DIR', requestId=request_id,
            msg='no records in event', rawEvent=json.dumps(event)[:1000]
            if isinstance(event, (dict, list)) else str(event)[:1000])
        return {'ok': 0, 'failed': 0, 'results': []}

    summary = {'ok': 0, 'skipped': 0, 'failed': 0}
    results = []
    retry_error = None      # 记住第一个可重试错误，本批处理完后再抛

    for record in records:
        base = {'requestId': request_id, 'srcBucket': SRC_BUCKET,
                'srcKey': record.get('key', '')}
        try:
            result, item = handle_one(record, request_id)
            item['result'] = result
            item['level'] = 'INFO'
            log(**item)
            results.append({'srcKey': item['srcKey'], 'result': result})
            summary['ok' if result == 'OK' else 'skipped'] += 1

        except SkipObject as e:
            base.update(result='SKIP_DIR', level='INFO', reason=str(e))
            log(**base)
            results.append({'srcKey': base['srcKey'], 'result': 'SKIP_DIR'})
            summary['skipped'] += 1

        except (NonRetryable, CosClientError) as e:
            # 业务错误 / 客户端参数错误：重试无意义
            base.update(result='FAILED', level='ERROR', retryable=False,
                        errorType=type(e).__name__, error=str(e))
            log(**base)
            results.append({'srcKey': base['srcKey'], 'result': 'FAILED'})
            summary['failed'] += 1

        except CosServiceError as e:
            code = e.get_error_code()
            status = e.get_status_code() or 0
            base.update(cosCode=code, cosStatus=status,
                        errorType='CosServiceError', error=str(e))
            fatal = code in NON_RETRYABLE_CODES or (400 <= status < 500 and status != 429)
            if fatal:
                base.update(result='FAILED', level='ERROR', retryable=False)
                log(**base)
                results.append({'srcKey': base['srcKey'], 'result': 'FAILED'})
                summary['failed'] += 1
            else:
                # 5xx / 429 -> 交平台重试
                base.update(result='RETRY', level='ERROR', retryable=True)
                log(**base)
                results.append({'srcKey': base['srcKey'], 'result': 'RETRY'})
                summary['failed'] += 1
                retry_error = retry_error or e

        except Exception as e:
            # 超时 / 连接中断 / 短读 / crc 不一致 -> 可重试
            base.update(result='RETRY', level='ERROR', retryable=True,
                        errorType=type(e).__name__, error=str(e))
            log(**base)
            results.append({'srcKey': base['srcKey'], 'result': 'RETRY'})
            summary['failed'] += 1
            retry_error = retry_error or e

    log(level='INFO', msg='batch_done', requestId=request_id, **summary)

    if retry_error is not None:
        # 抛出去让 SCF 异步重试（默认 2 次，间隔 1 分钟）
        # https://cloud.tencent.com/document/product/583/41138
        raise retry_error

    summary['results'] = results
    return summary
