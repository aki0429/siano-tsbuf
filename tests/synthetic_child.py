"""実機を開かずに合成 TS と異常な子プロセスを生成する。"""

import argparse
import os
import signal
import struct
import time
from pathlib import Path


def packet(index: int) -> bytes:
    # 固定 PID の payload に連番を入れ、脱落とバイト順序を検出できるようにする。
    header = bytes((0x47, 0x01, 0x00, 0x10 | (index & 15)))
    payload = struct.pack('>Q', index) + bytes((index % 251,)) * 176
    return header + payload


def payload(count: int, tail: int) -> bytes:
    return b''.join(packet(index) for index in range(count)) + packet(count)[:tail]


def cc_packet(index: int, cc: int, disc: bool = False) -> bytes:
    # CC (連続性カウンタ) 検証用のパケット。disc=True は adaptation field の
    # discontinuity_indicator を立てた「正規のジャンプ」を表す。
    if disc:
        header = bytes((0x47, 0x01, 0x00, 0x30 | (cc & 15), 0x01, 0x80))
        body = struct.pack('>Q', index) + bytes((index % 251,)) * 174
        return header + body
    header = bytes((0x47, 0x01, 0x00, 0x10 | (cc & 15)))
    body = struct.pack('>Q', index) + bytes((index % 251,)) * 176
    return header + body


def cc_stream(count: int, skip_at=None, skip_len=0, disc_at=None, disc_len=0, dup_at=None) -> bytes:
    # 意図的な CC ジャンプ (skip)・discontinuity 付きジャンプ (disc)・重複 (dup) を混ぜた
    # ストリームを作り、ラッパが数え分けられるかの入力にする。
    out = []
    for i in range(count):
        shift = skip_len if (skip_at is not None and i >= skip_at) else 0
        if disc_at is not None and i == disc_at:
            out.append(cc_packet(i, i + shift + disc_len, disc=True))
            continue
        if disc_at is not None and i > disc_at:
            shift += disc_len
        cc = i + shift
        out.append(cc_packet(i, cc))
        if dup_at is not None and i == dup_at:
            out.append(cc_packet(i, cc))
    return b''.join(out)


def write_all(fd: int, data: bytes) -> None:
    # pipe の short write はテスト対象ではなく生成側で正しく再送する。
    offset = 0
    while offset < len(data):
        offset += os.write(fd, data[offset:])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--packets', type=int, default=1000)
    parser.add_argument('--tail', type=int, default=0)
    parser.add_argument('--chunks', default='65536')
    parser.add_argument('--exit-code', type=int, default=0)
    parser.add_argument('--done')
    parser.add_argument('--pidfile')
    parser.add_argument('--hang', action='store_true')
    parser.add_argument('--cc-mode', choices=('gap', 'dup', 'disc'))
    args = parser.parse_args()
    if args.pidfile:
        Path(args.pidfile).write_text(str(os.getpid()), encoding='ascii')
    if args.cc_mode == 'gap':
        data = cc_stream(args.packets, skip_at=20, skip_len=3)
    elif args.cc_mode == 'dup':
        data = cc_stream(args.packets, dup_at=25)
    elif args.cc_mode == 'disc':
        data = cc_stream(args.packets, disc_at=10, disc_len=5)
    else:
        data = payload(args.packets, args.tail)
    chunks = [int(value) for value in args.chunks.split(',')]
    offset = 0
    index = 0
    while offset < len(data):
        size = chunks[index % len(chunks)]
        write_all(1, data[offset:offset + size])
        offset += size
        index += 1
    if args.done:
        Path(args.done).write_text('done\n', encoding='ascii')
    if args.hang:
        # チューナーの読み出しを模して、終了されるまで居座る
        while True:
            time.sleep(1)
    raise SystemExit(args.exit_code)


if __name__ == '__main__':
    main()
