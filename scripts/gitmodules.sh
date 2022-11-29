#!/bin/bash
hublist="
chestnut
new-york-university
yale-hiv
romi
new-york-state-psychiatric
tcu
bay-state
friends
yale
indiana-university
brown-university
university-of-kentucky
"
echo $hublist

for hub in $hublist
do
    git submodule add "ssh://git@rcg-git.uchicago.edu:443/maarc/dasc/hubs/${hub}.git" jcoin_hubs/${hub}
done