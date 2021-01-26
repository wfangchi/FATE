#
#  Copyright 2019 The FATE Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding


def loadprikey(keystr):
    keystr = keystr.strip()
    head = '-----BEGIN RSA PRIVATE KEY-----'
    end = '-----END RSA PRIVATE KEY-----'
    headlen = len(head)
    if not keystr.startswith('-----'):
        keystr = head + '\n' + keystr + '\n' + end
    if not keystr.startswith(head):
        raise Exception('key format error.')
    if keystr[headlen] != '\n' and keystr[headlen] != '\r':
        raise Exception('key format error.')
    key = serialization.load_pem_private_key(keystr.encode('utf-8'), password=None, backend=default_backend())
    return key


def loadpubkey(keystr):
    keystr = keystr.strip()
    head = '-----BEGIN PUBLIC KEY-----'
    end = '-----END PUBLIC KEY-----'
    headlen = len(head)
    if not keystr.startswith('-----'):
        keystr = head + '\n' + keystr + '\n' + end
    if not keystr.startswith(head):
        raise Exception('key format error.')
    if keystr[headlen] != '\n' and keystr[headlen] != '\r':
        raise Exception('key format error.')
    key = serialization.load_pem_public_key(keystr.encode('utf-8'), backend=default_backend())
    return key


def bytes_to_hex(s):
    return ''.join(['%02x'%(c) for c in s])


def rsaencrypt(pubkeystr, data):
    key = loadpubkey(pubkeystr)
    ret = key.encrypt(data.encode('utf-8'), padding.PKCS1v15())
    return bytes_to_hex(ret)


def rsadecrypt(prikeystr, data):
    key = loadprikey(prikeystr)
    data = bytes.fromhex(data)
    return key.decrypt(data, padding.PKCS1v15()).decode()


def pwencrypt(pubkeystr, data):
    ret = rsaencrypt(pubkeystr, data)
    ret = 'ffffff02' + ret
    return ret


def pwdecrypt(prikeystr, data):
    pwhead = 'ffffff02'
    if not data.startswith(pwhead):
        raise Exception('encrypted password format error.')
    data = data[len(pwhead):]
    return rsadecrypt(prikeystr, data)
