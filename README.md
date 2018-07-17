# Währungen

* Kurzbezeichung
  * EURCHF
  * EURCHF=X
  * 1 EUR = x CHF
* meint: Agentur gibt x CHF für 1 EUR.
  * Yahoo: EURCHF=X - Bid = Geldkurs = NOTEN, Ask = Briefkurs = DEVISEN
  * Bid = Verlange = Kaufangebot,  Ask = Biete = Verkaufsangebot

# Usage

Before executing any code: Check usage policy on fetched site. Acquire API-Keys where necessary.

## Unittests Scheduler

```python
python3 -munittest
```

## Testing jobs

```python
python3 -mjobs.esg
```

# Requirements

```
# (for influxdb) pyinflux (github.com/yvesf/pyinflux, without [parser]), not on pypi
currencies==2014.7.13
funcparserlib==0.3.6
lxml
```