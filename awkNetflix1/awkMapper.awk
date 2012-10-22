# awkMapper.awk written for netflixReorg dataset
# Mapper for netflix1
# Written by Joey Calca
# r3dfish@hackedexistence.com
# http://hackedexistence.com

# Tokenize input on ","
BEGIN { FS = "," }

# output token1 (tab) token3 "," token4
{ print $1 "\t" $3 "," $4 }