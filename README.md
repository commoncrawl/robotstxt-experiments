Robots.txt Experiments and Metrics
==================================

How is the Robots Exclusion Protocol (robots.txt or [RFC 9309](https://datatracker.ietf.org/doc/rfc9309/)) used in the WWW? This projects tries to get some insights mining Common Crawl's robots.txt captures of the years 2016 – 2024.

## Top-K Sampling of Web Sites

Three Tranco top-1M lists have been combined into a single ranked list, see [top-k-sites](./data/top-k-sites/README.md).
The resulting list of 2 million web sites is used to obtain samples on multiple strata (1k, 5k, 10k, 100k, 1M, 2M).

## Locating and Downloading Robots.txt Captures in Common Crawl's Web Archives

Common Crawl's Web Archives include since 2016 a [robots.txt data set](https://commoncrawl.org/2016/09/robotstxt-and-404-redirect-data-sets/)
from which the robots.txt captures are extracted. This is done utilizing the
[columnar URL index](https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/).
The necessary steps are described in the [data preparation notebook](./src/jupyter/data-preparation-top-k-sample.ipynb).

## Metrics and Findings

- [top-k metrics notebook](./src/jupyter/metrics-top-k-sample.ipynb): first aggregations and few plots
- [user-agent metrics notebook](./src/jupyter/metrics-user-agents.ipynb): more plots about user-agents addressed in robots.txt files

## Poster at IIPC Web Archiving Conference 2025

Condensed results of this project were presented as poster on the [IIPC Web Archiving Conference 2025](https://netpreserve.org/ga2025/).
A copy of the poster is available [here](./docs/robotstxt-crawler-politeness-wac2025-sn-tv.pdf).

## Notes and Credits

This project is an extension of work done for a presentation at #ossym2022:
"[The robots.txt standard – Implementations and Usage](https://indico.cern.ch/event/1149330/contributions/5074600/)".
The corresponding code is found at [ossym2022-robotstxt-experiments](https://github.com/sebastian-nagel/ossym2022-robotstxt-experiments).

The idea to look at multiple strata (top-k) is inspired by the work of Longpre et al. "Consent in crisis" (<https://arxiv.org/abs/2407.14933>)
and Liu et al. "Somesite I used to crawl" (<https://arxiv.org/pdf/2411.15091>).
