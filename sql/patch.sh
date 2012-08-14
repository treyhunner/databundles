#/bin/bash 

d=$(dirname $0)
echo $d

cp $d/configuration-pg.dez $d/configuration-sqlite.dez
patch $d/configuration-sqlite.dez < $d/patch


