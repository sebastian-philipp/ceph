#!/usr/bin/env bash

target_fpath=${1:-cephadm}

cp cephadm.py __main__.py
zip ca.zip __main__.py
echo '#!/usr/bin/env python3' | cat - ca.zip >cephadm
chmod +x cephadm
rm __main__.py ca.zip
if [ "${target_fpath}" != "cephadm" ]; then
    mv cephadm ${target_fpath}
fi
