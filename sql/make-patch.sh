#/bin/bash 

d=$(dirname $0)
echo $d

diff -u $d/configuration-pg.dez $d/configuration-sqlite.dez > patch

