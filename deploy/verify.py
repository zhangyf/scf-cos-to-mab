# -*- coding: utf-8 -*-
"""
上线前实测脚本：验证元数据加速桶(MAB)的实际行为。

【为什么需要这个脚本】
方案里有 7 项行为官方文档没有明确说明，只能实测。这些项直接决定
index.py 的环境变量该怎么配。不跑这个脚本就上线，等于赌运气。

【用法】
    export COS_SECRET_ID=xxx
    export COS_SECRET_KEY=xxx
    export SRC_BUCKET=srcbucket-1250000000
    export DST_BUCKET=dstmab-1250000000
    export COS_REGION=ap-tokyo
    python verify.py

    # 只跑指定项：
    python verify.py 1 3 5
    # 跳过耗时的大文件分块测试：
    export SKIP_LARGE=true && python verify.py

【安全性】
- 所有测试对象都写在 VERIFY_PREFIX（默认 _verify_tmp/）下，不碰业务数据
- 源桶只做写入测试对象 + 读取，不删除任何已有对象
- MAB 不支持 DELETE Object，测试残留对象需手工清理，脚本会打印清单
"""

import json
import os
import sys
import time
import uuid

try:
    from qcloud_cos import CosConfig, CosS3Client
    from qcloud_cos.cos_exception import CosServiceError, CosClientError
except ImportError:
    print('缺少依赖，请先安装：pip install cos-python-sdk-v5')
    sys.exit(1)

import logging
logging.getLogger('qcloud_cos').setLevel(logging.ERROR)


# ============================ 配置 ============================

REGION = os.environ.get('COS_REGION', 'ap-tokyo')
SRC_BUCKET = os.environ.get('SRC_BUCKET', '')
DST_BUCKET = os.environ.get('DST_BUCKET', '')
SECRET_ID = os.environ.get('COS_SECRET_ID')
SECRET_KEY = os.environ.get('COS_SECRET_KEY')
TOKEN = os.environ.get('COS_SESSION_TOKEN')

VERIFY_PREFIX = os.environ.get('VERIFY_PREFIX', '_verify_tmp/')
SKIP_LARGE = os.environ.get('SKIP_LARGE', 'false').lower() in ('true', '1', 'yes')

CRC_HEADER = 'x-cos-hash-crc64ecma'
RUN_ID = time.strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:6]

# 记录写进 MAB 的对象，最后统一打印（MAB 删不掉，需手工清理）
DST_LEFTOVER = []
SRC_LEFTOVER = []


def _fail_config():
    missing = [n for n, v in [
        ('SRC_BUCKET', SRC_BUCKET), ('DST_BUCKET', DST_BUCKET),
        ('COS_SECRET_ID', SECRET_ID), ('COS_SECRET_KEY', SECRET_KEY),
    ] if not v]
    if missing:
        print('缺少环境变量：' + ', '.join(missing))
        sys.exit(1)


def make_client(bucket=None, internal=False):
    kwargs = {'Region': REGION, 'SecretId': SECRET_ID,
              'SecretKey': SECRET_KEY, 'Scheme': 'https'}
    if TOKEN:
        kwargs['Token'] = TOKEN
    if internal and bucket:
        kwargs['Domain'] = '{0}.cos-internal.{1}.tencentcos.cn'.format(bucket, REGION)
        kwargs['Scheme'] = 'http'
    return CosS3Client(CosConfig(**kwargs))


# ============================ 报告 ============================

RESULTS = []


def record(no, name, verdict, detail, action):
    """verdict: PASS / FAIL / WARN / SKIP"""
    RESULTS.append({'no': no, 'name': name, 'verdict': verdict,
                    'detail': detail, 'action': action})
    mark = {'PASS': '[ OK ]', 'FAIL': '[FAIL]',
            'WARN': '[WARN]', 'SKIP': '[SKIP]'}[verdict]
    print('{0} {1}. {2}'.format(mark, no, name))
    if detail:
        print('       {0}'.format(detail))
    if action:
        print('       -> {0}'.format(action))


def head_or_none(client, bucket, key):
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except CosServiceError as e:
        if e.get_status_code() == 404 or e.get_error_code() in ('NoSuchKey', 'NoSuchResource'):
            return None
        raise


def put_src(client, key, data):
    client.put_object(Bucket=SRC_BUCKET, Key=key, Body=data)
    SRC_LEFTOVER.append(key)


def put_dst(client, key, data):
    resp = client.put_object(Bucket=DST_BUCKET, Key=key, Body=data)
    DST_LEFTOVER.append(key)
    return resp


# ============================ 测试项 ============================

def test_1_crc64(src, dst):
    """MAB 的 Put 响应和 Head 响应是否返回 x-cos-hash-crc64ecma"""
    name = 'MAB 是否返回 crc64（决定 VERIFY_CRC 能否开启）'
    key = '{0}{1}/crc-probe.bin'.format(VERIFY_PREFIX, RUN_ID)
    data = b'crc64-probe-' + os.urandom(1024)
    try:
        put_resp = put_dst(dst, key, data)
        put_crc = put_resp.get(CRC_HEADER)
        head = head_or_none(dst, DST_BUCKET, key)
        head_crc = (head or {}).get(CRC_HEADER)

        if put_crc and head_crc:
            record(1, name, 'PASS',
                   'Put 与 Head 均返回 crc64（put={0}）'.format(put_crc),
                   '保持 VERIFY_CRC=true')
        elif put_crc and not head_crc:
            record(1, name, 'WARN',
                   'Put 返回 crc64 但 Head 不返回',
                   'VERIFY_CRC 可保持 true（校验只用 Put 响应），但幂等判断会退化为只比 size')
        else:
            record(1, name, 'WARN',
                   'Put 未返回 crc64（put={0} head={1}）'.format(put_crc, head_crc),
                   '设 VERIFY_CRC=false，改为只比对 size')
    except Exception as e:
        record(1, name, 'FAIL', '{0}: {1}'.format(type(e).__name__, e),
               '先解决 MAB 写入权限或连通性问题')


def test_2_internal_domain(src, dst):
    """MAB 是否支持内网域名 <bucket>.cos-internal.<region>.tencentcos.cn"""
    name = 'MAB 内网域名可达性（决定 DST_USE_INTERNAL_DOMAIN）'
    key = '{0}{1}/internal-probe.bin'.format(VERIFY_PREFIX, RUN_ID)
    try:
        icli = make_client(DST_BUCKET, internal=True)
        icli.put_object(Bucket=DST_BUCKET, Key=key, Body=b'internal-domain-probe')
        DST_LEFTOVER.append(key)
        record(2, name, 'PASS', '内网域名写入成功',
               '可设 DST_USE_INTERNAL_DOMAIN=true 省流量费（注意：只有在 SCF 环境内才真正走内网）')
    except Exception as e:
        record(2, name, 'WARN', '{0}: {1}'.format(type(e).__name__, str(e)[:200]),
               '保持 DST_USE_INTERNAL_DOMAIN=false。'
               '注意本机不在腾讯云内网，失败属预期，需在 SCF 里复测才有结论')


def test_3_multipart(src, dst):
    """MAB 的 upload_part（非 Copy）是否可用 —— 大文件路径依赖这个"""
    name = 'MAB 分块上传 upload_part 可用性（决定大文件路径）'
    if SKIP_LARGE:
        record(3, name, 'SKIP', 'SKIP_LARGE=true', '需要时去掉该环境变量重跑')
        return

    key = '{0}{1}/multipart-probe.bin'.format(VERIFY_PREFIX, RUN_ID)
    part_size = 1024 * 1024        # 1MB/块，仅验证接口可用性，不追求真实规模
    upload_id = None
    try:
        created = dst.create_multipart_upload(Bucket=DST_BUCKET, Key=key)
        upload_id = created['UploadId']
        parts = []
        for i in (1, 2):
            chunk = os.urandom(part_size)
            r = dst.upload_part(Bucket=DST_BUCKET, Key=key, Body=chunk,
                                PartNumber=i, UploadId=upload_id)
            parts.append({'PartNumber': str(i), 'ETag': r['ETag']})
        dst.complete_multipart_upload(
            Bucket=DST_BUCKET, Key=key, UploadId=upload_id,
            MultipartUpload={'Part': parts})
        DST_LEFTOVER.append(key)

        head = head_or_none(dst, DST_BUCKET, key)
        got = int((head or {}).get('Content-Length', -1))
        if got == part_size * 2:
            record(3, name, 'PASS', '两块各 1MB 上传并合并成功，大小校验通过',
                   '大文件路径可用，MAX_OBJECT_GB 可保持 48')
        else:
            record(3, name, 'FAIL', '合并后大小异常：{0}，期望 {1}'.format(got, part_size * 2),
                   '把 SMALL_MB 设得足够大让所有对象走内存直传，或限制 MAX_OBJECT_GB')
    except Exception as e:
        if upload_id:
            try:
                dst.abort_multipart_upload(Bucket=DST_BUCKET, Key=key, UploadId=upload_id)
            except Exception:
                pass
        record(3, name, 'FAIL', '{0}: {1}'.format(type(e).__name__, str(e)[:200]),
               '大文件路径不可用。把 SMALL_MB 调到覆盖全部对象，'
               '并把 MAX_OBJECT_GB 降到 SMALL_MB 同值，让超限对象直接判 FAILED')


def test_4_deep_dir_concurrent(src, dst):
    """并发写同一深层目录，验证 MAB 递归建目录是否冲突"""
    name = '并发写同一深层目录（MAB 递归建目录是否冲突）'
    from concurrent.futures import ThreadPoolExecutor
    base = '{0}{1}/2026/08/20/concurrent/'.format(VERIFY_PREFIX, RUN_ID)
    n = 20

    def one(i):
        k = '{0}part-{1:03d}.log'.format(base, i)
        try:
            dst.put_object(Bucket=DST_BUCKET, Key=k, Body=b'x' * 256)
            return (k, None)
        except Exception as e:
            return (k, '{0}: {1}'.format(type(e).__name__, str(e)[:120]))

    try:
        with ThreadPoolExecutor(max_workers=n) as pool:
            out = list(pool.map(one, range(n)))
        errs = [e for _, e in out if e]
        for k, e in out:
            if not e:
                DST_LEFTOVER.append(k)
        if not errs:
            record(4, name, 'PASS',
                   '{0} 个对象并发写入同一深层目录全部成功'.format(n),
                   '未发现建目录冲突。上线后仍需按真实量级观察')
        else:
            record(4, name, 'WARN',
                   '{0}/{1} 失败，样例：{2}'.format(len(errs), n, errs[0]),
                   '若为 5xx 则会走平台重试，可接受；'
                   '若为 409/冲突类错误，需考虑预建目录或降低并发')
    except Exception as e:
        record(4, name, 'FAIL', '{0}: {1}'.format(type(e).__name__, e), '检查权限与连通性')


def test_5_special_chars(src, dst):
    """含中文 / 加号 / 空格的 key 能否正常往返 —— 决定 DECODE_PLUS"""
    name = '特殊字符 key 往返（决定 DECODE_PLUS）'
    cases = {
        'chinese': '日志-中文.log',
        'plus': 'a+b.log',
        'space': 'a b.log',
        'mixed': '接入层 access+01.log',
    }
    ok, bad = [], []
    for tag, fname in cases.items():
        key = '{0}{1}/chars/{2}'.format(VERIFY_PREFIX, RUN_ID, fname)
        try:
            dst.put_object(Bucket=DST_BUCKET, Key=key, Body=b'probe')
            DST_LEFTOVER.append(key)
            if head_or_none(dst, DST_BUCKET, key) is not None:
                ok.append(tag)
            else:
                bad.append('{0}(head 404)'.format(tag))
        except Exception as e:
            bad.append('{0}({1})'.format(tag, type(e).__name__))

    if not bad:
        record(5, name, 'PASS', '全部通过：' + ', '.join(ok),
               'MAB 侧无字符限制。注意 DECODE_PLUS 影响的是【解析 COS event】环节，'
               '若源文件名含 + 号，必须设 DECODE_PLUS=false，否则 + 会被解成空格')
    else:
        record(5, name, 'WARN', '失败项：' + ', '.join(bad),
               '避免在这些字符上依赖 MAB，或与日志写入方约定文件名规范')


def test_6_dir_file_conflict(src, dst):
    """目录与同名文件冲突时 MAB 的行为"""
    name = '目录与同名文件冲突'
    base = '{0}{1}/conflict'.format(VERIFY_PREFIX, RUN_ID)
    try:
        # 先通过带中间路径的 Put 让 MAB 创建目录 base/
        child = base + '/child.log'
        dst.put_object(Bucket=DST_BUCKET, Key=child, Body=b'child')
        DST_LEFTOVER.append(child)

        # 再尝试把 base 本身当成文件写入
        try:
            dst.put_object(Bucket=DST_BUCKET, Key=base, Body=b'as-file')
            DST_LEFTOVER.append(base)
            record(6, name, 'WARN',
                   '目录已存在时仍允许写入同名文件，MAB 未报错',
                   '需与日志写入方确认不会产生这类 key，否则可能出现难解释的结构')
        except CosServiceError as e:
            record(6, name, 'PASS',
                   '按预期拒绝：{0} / {1}'.format(e.get_status_code(), e.get_error_code()),
                   '此类错误已归入 index.py 的不可重试分支 -> FAILED 日志')
    except Exception as e:
        record(6, name, 'FAIL', '{0}: {1}'.format(type(e).__name__, e), '检查权限')


def test_7_end_to_end(src, dst):
    """端到端：模拟 index.py 的搬运逻辑，源桶写入 -> 读出 -> 写 MAB -> 校验一致"""
    name = '端到端搬运一致性（模拟 index.py 主路径）'
    rel = '2026/08/20/e2e-probe.log.gz'
    src_key = '{0}{1}/prod-logs/{2}'.format(VERIFY_PREFIX, RUN_ID, rel)
    dst_key = '{0}{1}/prod-raw-logs/{2}'.format(VERIFY_PREFIX, RUN_ID, rel)
    payload = os.urandom(64 * 1024)

    try:
        put_src(src, src_key, payload)
        src_head = head_or_none(src, SRC_BUCKET, src_key)
        src_size = int(src_head.get('Content-Length', -1))
        src_crc = src_head.get(CRC_HEADER)

        # 完全复刻 move_inline 的做法
        body = src.get_object(Bucket=SRC_BUCKET, Key=src_key)['Body'].get_raw_stream().read()
        if len(body) != src_size:
            record(7, name, 'FAIL',
                   '源读取字节数不符：{0} vs {1}'.format(len(body), src_size), '排查网络')
            return
        put_resp = put_dst(dst, dst_key, body)

        dst_head = head_or_none(dst, DST_BUCKET, dst_key)
        dst_size = int((dst_head or {}).get('Content-Length', -1))
        dst_crc = (put_resp or {}).get(CRC_HEADER) or (dst_head or {}).get(CRC_HEADER)

        size_ok = (dst_size == src_size)
        crc_ok = (not src_crc or not dst_crc or str(src_crc) == str(dst_crc))

        if size_ok and crc_ok:
            record(7, name, 'PASS',
                   'size={0} 一致，crc64 {1}'.format(
                       src_size, '一致' if (src_crc and dst_crc) else '不可比（一侧缺失）'),
                   '主搬运路径可用')
        else:
            record(7, name, 'FAIL',
                   'size {0}/{1}，crc src={2} dst={3}'.format(
                       src_size, dst_size, src_crc, dst_crc),
                   '数据不一致，不要上线，先排查')
    except Exception as e:
        record(7, name, 'FAIL', '{0}: {1}'.format(type(e).__name__, str(e)[:200]),
               '主路径不通，检查两个桶的权限配置')


TESTS = [
    (1, test_1_crc64),
    (2, test_2_internal_domain),
    (3, test_3_multipart),
    (4, test_4_deep_dir_concurrent),
    (5, test_5_special_chars),
    (6, test_6_dir_file_conflict),
    (7, test_7_end_to_end),
]


def main():
    _fail_config()
    wanted = set()
    for a in sys.argv[1:]:
        if a.isdigit():
            wanted.add(int(a))

    print('=' * 68)
    print(' MAB 行为实测')
    print(' region     : {0}'.format(REGION))
    print(' src bucket : {0}'.format(SRC_BUCKET))
    print(' dst bucket : {0} (元数据加速桶)'.format(DST_BUCKET))
    print(' test prefix: {0}{1}/'.format(VERIFY_PREFIX, RUN_ID))
    print('=' * 68)
    print()

    src = make_client(SRC_BUCKET)
    dst = make_client(DST_BUCKET)

    for no, fn in TESTS:
        if wanted and no not in wanted:
            continue
        try:
            fn(src, dst)
        except Exception as e:
            record(no, fn.__doc__ or fn.__name__, 'FAIL',
                   'uncaught {0}: {1}'.format(type(e).__name__, e), '脚本自身异常，请反馈')
        print()

    # ---- 汇总 ----
    print('=' * 68)
    counts = {}
    for r in RESULTS:
        counts[r['verdict']] = counts.get(r['verdict'], 0) + 1
    print(' 汇总: ' + '  '.join('{0}={1}'.format(k, counts[k]) for k in sorted(counts)))
    print('=' * 68)

    blockers = [r for r in RESULTS if r['verdict'] == 'FAIL']
    if blockers:
        print('\n阻塞项（必须先解决）：')
        for r in blockers:
            print('  {0}. {1}\n     {2}\n     -> {3}'.format(
                r['no'], r['name'], r['detail'], r['action']))

    warns = [r for r in RESULTS if r['verdict'] == 'WARN']
    if warns:
        print('\n需要据此调整环境变量：')
        for r in warns:
            print('  {0}. {1}\n     -> {2}'.format(r['no'], r['name'], r['action']))

    # ---- 残留清理提示 ----
    if DST_LEFTOVER:
        print('\n【重要】MAB 不支持 DELETE Object，以下测试对象需手工清理：')
        print('  桶: {0}'.format(DST_BUCKET))
        print('  路径: {0}{1}/'.format(VERIFY_PREFIX, RUN_ID))
        print('  共 {0} 个对象。可在控制台按该前缀批量删除，'
              '或用生命周期规则对 {1} 前缀设置过期。'.format(
                  len(DST_LEFTOVER), VERIFY_PREFIX))
    if SRC_LEFTOVER:
        print('\n源桶（普通桶，可删）测试对象 {0} 个，清理命令：'.format(len(SRC_LEFTOVER)))
        print('  coscmd -b {0} -r {1} delete -r {2}{3}/'.format(
            SRC_BUCKET, REGION, VERIFY_PREFIX, RUN_ID))

    # 机器可读结果，便于留档
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       'verify-result-{0}.json'.format(RUN_ID))
    try:
        with open(out, 'w', encoding='utf-8') as f:
            json.dump({'runId': RUN_ID, 'region': REGION,
                       'srcBucket': SRC_BUCKET, 'dstBucket': DST_BUCKET,
                       'results': RESULTS, 'dstLeftover': DST_LEFTOVER},
                      f, ensure_ascii=False, indent=2)
        print('\n结果已存档：{0}'.format(out))
    except Exception:
        pass

    sys.exit(1 if blockers else 0)


if __name__ == '__main__':
    main()
