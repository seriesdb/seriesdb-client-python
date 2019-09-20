#!/bin/bash

current_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )";
cd ${current_dir}
echo ${current_dir}
# Fetch proto files
if [[ ! -d proto ]]; then
    git clone https://github.com/seriesdb/seriesdb-protocol.git proto;
fi

# Init packages
mkdir -p seriesdb/protocol
touch seriesdb/__init__.py
touch seriesdb/protocol/__init__.py

echo "try:
  __import__('pkg_resources').declare_namespace(__name__)
except ImportError:
  __path__ = __import__('pkgutil').extend_path(__path__, __name__)
" > seriesdb/__init__.py

# Generate xxx_pb2 file by protoc
protoc -I=proto --python_out=seriesdb/protocol seriesdb_protocol.proto

# Generate xxx_ext file
bin/gen_protocol_ext.py \
    --proto_file proto/seriesdb_protocol.proto \
    --enum_type_name MsgType
