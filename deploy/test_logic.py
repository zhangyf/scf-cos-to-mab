# -*- coding: utf-8 -*-
"""
纯逻辑单元测试：只测不依赖 cos SDK 的部分。

用途：验证 event 解析和路径映射这两块最容易出错的逻辑。
运行：python deploy/test_logic.py
不需要安装 cos-python-sdk-v5，也不需要任何云端凭证。
"""

import os
import sys
from urllib.parse import unquote_plus

# ---- 复制 index.py 中不依赖 SDK 的逻辑，保持完全一致 ----

SRC_BUCKET = 'srcbucket-1250000000'


def _norm_prefix(raw, default):
    p = (raw or default).strip().lstrip('/')
    if p and not p.endswith('/'):
        p += '/'
    return p


SRC_PREFIX = _norm_prefix(None, 'prod-logs/')
DST_PREFIX = _norm_prefix(None, 'prod-raw-logs/')


class NonRetryable(Exception):
    pass


class SkipObject(Exception):
    pass


def parse_event(event):
    if isinstance(event, dict) and event.get('keys'):
        out = []
        for k in event['keys']:
            key = str(k).lstrip('/')
            if key.startswith(SRC_BUCKET + '/'):
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
        key = unquote_plus(str(obj.get('key', '')))
        key = key.lstrip('/')
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
    if SRC_PREFIX and not src_key.startswith(SRC_PREFIX):
        raise NonRetryable('key not under SRC_PREFIX({0}): {1}'.format(SRC_PREFIX, src_key))
    rel = src_key[len(SRC_PREFIX):] if SRC_PREFIX else src_key
    if not rel:
        raise SkipObject('empty relative key')
    if rel.endswith('/'):
        raise SkipObject('directory placeholder')
    return DST_PREFIX + rel


# ---- 测试 ----

PASS = 0
FAIL = 0


def check(name, got, want):
    global PASS, FAIL
    if got == want:
        PASS += 1
        print('  PASS  {0}'.format(name))
    else:
        FAIL += 1
        print('  FAIL  {0}\n        got : {1!r}\n        want: {2!r}'.format(name, got, want))


def check_raises(name, fn, exc):
    global PASS, FAIL
    try:
        fn()
    except exc:
        PASS += 1
        print('  PASS  {0}'.format(name))
        return
    except Exception as e:
        FAIL += 1
        print('  FAIL  {0}\n        raised {1} instead of {2}'.format(
            name, type(e).__name__, exc.__name__))
        return
    FAIL += 1
    print('  FAIL  {0}\n        no exception raised, expected {1}'.format(name, exc.__name__))


def cos_event(key, appid='1250000000', bucket='srcbucket', size=1029):
    """构造一条与官方文档一致的 COS 触发器事件"""
    return {
        'Records': [{
            'cos': {
                'cosSchemaVersion': '1.0',
                'cosObject': {
                    'url': 'http://example.com/x',
                    'meta': {'x-cos-request-id': 'REQ-ABC', 'Content-Type': ''},
                    'vid': '',
                    'key': key,
                    'size': size,
                },
                'cosBucket': {'region': 'ap-tokyo', 'name': bucket, 'appid': appid},
                'cosNotificationId': 'unkown',
            },
            'event': {'eventName': 'cos:ObjectCreated:Put', 'eventTime': 1787000000},
        }]
    }


print('\n[1] 前缀规范化 —— 用户写的 "/prod-logs" 必须被纠正成 "prod-logs/"')
check('去掉开头斜杠并补尾斜杠', _norm_prefix('/prod-logs', 'x/'), 'prod-logs/')
check('已规范的保持不变', _norm_prefix('prod-logs/', 'x/'), 'prod-logs/')
check('两端都要处理', _norm_prefix('  /prod-raw-logs  ', 'x/'), 'prod-raw-logs/')
check('为空时用默认值', _norm_prefix(None, 'prod-logs/'), 'prod-logs/')

print('\n[2] COS event 解析 —— 剥掉 /appid/bucketname/ 前缀')
recs = parse_event(cos_event('/1250000000/srcbucket/prod-logs/2026/08/20/app.log.gz'))
check('对象键正确剥离', recs[0]['key'], 'prod-logs/2026/08/20/app.log.gz')
check('size 透传', recs[0]['size'], 1029)
check('eventName 透传', recs[0]['eventName'], 'cos:ObjectCreated:Put')
check('cos requestId 透传', recs[0]['cosReqId'], 'REQ-ABC')

print('\n[3] URL 编码的 key')
recs = parse_event(cos_event('/1250000000/srcbucket/prod-logs/2026/08/20/%E6%97%A5%E5%BF%97.log'))
check('中文 key 解码', recs[0]['key'], 'prod-logs/2026/08/20/日志.log')
recs = parse_event(cos_event('/1250000000/srcbucket/prod-logs/2026/08/20/a+b.log'))
check('加号被 unquote_plus 转成空格（已知行为，需实测确认）',
      recs[0]['key'], 'prod-logs/2026/08/20/a b.log')

print('\n[4] 多级路径全部保留')
recs = parse_event(cos_event('/1250000000/srcbucket/prod-logs/2026/08/20/nginx/node-01/access.log'))
check('深层目录保留', recs[0]['key'], 'prod-logs/2026/08/20/nginx/node-01/access.log')

print('\n[5] 手动重放入参')
recs = parse_event({'keys': ['prod-logs/2026/08/20/a.gz', '/prod-logs/2026/08/20/b.gz',
                             'srcbucket-1250000000/prod-logs/2026/08/20/c.gz']})
check('三种写法都归一', [r['key'] for r in recs],
      ['prod-logs/2026/08/20/a.gz', 'prod-logs/2026/08/20/b.gz', 'prod-logs/2026/08/20/c.gz'])
check('重放标记 eventName', recs[0]['eventName'], 'manual:Replay')

print('\n[6] 空/异常 event 不炸')
check('空 dict', parse_event({}), [])
check('Records 为 None', parse_event({'Records': None}), [])
check('Records 内含 None', len(parse_event({'Records': [None]})), 1)

print('\n[7] 路径映射 —— 核心需求')
check('基本映射', map_key('prod-logs/2026/08/20/xxxx'), 'prod-raw-logs/2026/08/20/xxxx')
check('日期层级保留', map_key('prod-logs/2026/08/20/app.log.gz'),
      'prod-raw-logs/2026/08/20/app.log.gz')
check('深层目录保留', map_key('prod-logs/2026/08/20/nginx/node-01/access.log'),
      'prod-raw-logs/2026/08/20/nginx/node-01/access.log')
check('文件名含点号', map_key('prod-logs/2026/08/20/a.b.c.tar.gz'),
      'prod-raw-logs/2026/08/20/a.b.c.tar.gz')
check('中文文件名', map_key('prod-logs/2026/08/20/日志.log'),
      'prod-raw-logs/2026/08/20/日志.log')

print('\n[8] 路径映射的边界情况')
check_raises('目录占位对象 -> SkipObject',
             lambda: map_key('prod-logs/2026/08/20/'), SkipObject)
check_raises('前缀自身 -> SkipObject',
             lambda: map_key('prod-logs/'), SkipObject)
check_raises('不在源前缀下 -> NonRetryable',
             lambda: map_key('other-logs/2026/08/20/a.gz'), NonRetryable)
check_raises('相似但不匹配的前缀 -> NonRetryable',
             lambda: map_key('prod-logs-backup/2026/a.gz'), NonRetryable)
check('前缀名出现在中间层级也不受影响',
      map_key('prod-logs/2026/prod-logs/a.gz'), 'prod-raw-logs/2026/prod-logs/a.gz')

print('\n[9] 端到端：event -> key -> dst_key')
recs = parse_event(cos_event('/1250000000/srcbucket/prod-logs/2026/08/20/xxxx'))
check('完整链路', map_key(recs[0]['key']), 'prod-raw-logs/2026/08/20/xxxx')

print('\n' + '=' * 52)
print('  passed: {0}   failed: {1}'.format(PASS, FAIL))
print('=' * 52)
sys.exit(1 if FAIL else 0)
