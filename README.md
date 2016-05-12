# bop_naive

bop2016 naive

## Guide

You need at least python3.5 and package aiohttp to run.

## Possible paths

### Paper -> Paper

* Paper -> Paper

* Paper -> Paper -> Paper
* Paper -> Author -> Paper
* Paper -> F/J/C -> Paper

* Paper -> Paper -> Paper -> Paper
* Paper -> Paper -> Author -> Paper
* Paper -> Paper -> F/J/C -> Paper
* Paper -> Author -> Paper -> Paper
* Paper -> F/J/C -> Paper -> Paper

### Paper -> Author

* Paper -> Author

* Paper -> Paper -> Author

* Paper -> Paper -> Paper -> Author
* Paper -> F/J/C -> Paper -> Author
* Paper -> Author -> Paper -> Author
* Paper -> Author -> Affiliation -> Author

### Author -> Paper

* Author -> Paper

* Author -> Paper -> Paper

* Author -> Affiliation -> Author -> Paper
* Author -> Paper -> Paper -> Paper
* Author -> Paper -> F/J/C -> Paper
* Author -> Paper -> Author -> Paper

### Author -> Author

* Author -> Paper -> Author
* Author -> Affiliation -> Author

* Author -> Paper -> Paper -> Author

## Tests

### Number pairs

* 2251253715 2180737804 (author -> paper, 14)
* 2147152072 189831743 (paper -> paper, 18)
* 2332023333 2310280492 (paper -> paper, 1)
* 2332023333 57898110 (paper -> author, 1)
* 57898110 2014261844 (author -> author, 26)
* (additional) 2037897188 2058733183 (paper -> paper, 313)

### URLs

* http://127.0.0.1:8080/bop?id1=2251253715&id2=2180737804
* http://127.0.0.1:8080/bop?id1=2147152072&id2=189831743
* http://127.0.0.1:8080/bop?id1=2332023333&id2=2310280492
* http://127.0.0.1:8080/bop?id1=2332023333&id2=57898110
* http://127.0.0.1:8080/bop?id1=57898110&id2=2014261844
* http://127.0.0.1:8080/bop?id1=2037897188&id2=2058733183
