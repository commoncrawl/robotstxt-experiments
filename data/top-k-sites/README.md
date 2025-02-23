Selection of top-k sites
========================

Three Tranco top-1M lists have been combined into a single list.

The following lists were manually downloaded from the [Tranco](https://tranco-list.eu/) website:
- 2020-08-15
  - "This list aggregates the ranks from the lists provided by Alexa, Umbrella, and Majestic from 17 July 2020 to 15 August 2020 (30 days)."
  - only pay-level domains: [Z7NG](https://tranco-list.eu/list/Z7NG/1000000)
  - with subdomains: [35PL](https://tranco-list.eu/list/35PL/1000000)
- 2022-08-15
  - "This list aggregates the ranks from the lists provided by Alexa, Umbrella, Majestic, and Farsight from 17 July 2022 to 15 August 2022 (30 days)"
  - only pay-level domains: [PZQZJ](https://tranco-list.eu/list/PZQZJ/1000000)
  - with subdomains: [Y5N5G](https://tranco-list.eu/list/Y5N5G/1000000)
- 2024-08-15
  - "This list aggregates the ranks from the lists provided by Crux, Farsight, Majestic, Radar, and Umbrella from 17 July 2024 to 15 August 2024 (30 days)."
  - only pay-level domains [KJ3KW](https://tranco-list.eu/list/KJ3KW/1000000)
  - with subdomains: [V9KLN](https://tranco-list.eu/list/V9KLN/1000000)

The downloaded lists are placed into the following folder structure:
```
tranco/
├── 2020-08-15-paylevel-domain
│   └── tranco_Z7NG-1m.csv.zip
├── 2020-08-15-subdomain
│   └── tranco_35PL-1m.csv.zip
├── 2022-08-15-paylevel-domain
│   └── tranco_PZQZJ-1m.csv.zip
├── 2022-08-15-subdomain
│   └── tranco_Y5N5G-1m.csv.zip
├── 2024-08-15-paylevel-domain
│   └── tranco_KJ3KW-1m.csv.zip
└── 2024-08-15-subdomain
    └── tranco_V9KLN-1m.csv.zip
```

The lists "with subdomains" are combined into one list by running

    ./tranco/combine_tranco.sh

in the directory where this README is located.

The Tranco lists are combined into a single ranked lists using the [Dowdall rule](https://en.wikipedia.org/wiki/Borda_count#Dowdall), the same method which used to combine Tranco's sources into the Tranco list. See the scripts [combine_ranked_lists.py](./combine_ranked_lists.py) and [combine_tranco.sh](./tranco/combine_tranco.sh) for details of the implementation.

The resulting list is found at [tranco/tranco_combined.txt.gz](./tranco/tranco_combined.txt.gz). With the given input it includes 2,042,066 internet host names.