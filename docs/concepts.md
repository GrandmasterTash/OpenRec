# OpenRec Concepts

<img src="concepts.png" align="right" style="padding-right: 5px" width="300px"/>

A financial **Control** in OpenRec is configured via a single [yaml](https://en.wikipedia.org/wiki/YAML) file called a **charter** and requires a folder structure where data will be imported, manipulated and reconciled.

The charters can be registered in a central registry yaml file - this file is used to the monitoring and processing of the controls.

## Modules

<img src="modules.png" align="right" style="padding-right: 5px" width="400px"/>

OpenRec is composed of a number of sub-modules.
- **Steward** - Steward is the OpenRec orchestration application. Using the register, Steward will monitor control inboxes for new data and initiate the other OpenRec components to perform a match job. Steward is also responsible for ensuring only one match job per control is invoked at any one time and that metrics are exposed to a [Prometheus](https://prometheus.io/) server (if configured - via a [Pushgateway](https://github.com/prometheus/pushgateway)).
- **Jetwash** - Jetwash is component which pre-processes and cleans the inbox data, trimming whitespace, converting dates to ISO8601 format, etc. As well as adding a schema row (see [File Format](file_format.md)) and delivers well-formatted data to Celerity.
- **Celerity** - Celerity is the matching engine ingests data from Jetwash and combines it with any previous unmatched data to groups it and evaluate it against defined matching rules. Matched data is 'released' leaving only un-matched data behind.

## Folder Structure

## Virtual Grid