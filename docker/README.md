# Getting Started

To try some of the example controls in docker you must use the following (assumes you have docker and docker-compose installed): -

```bash
cd docker
docker-compose build
./openrec.sh
```

This will build OpenRec (first time will take a LONG time), then run Steward with a few of the example controls running.

You can use copy data from the /examples/data folder into the appropriate controls inbox to trigger a match job. The inboxs are in the /docker/data folders where you'll see a folder per control in the /docker/etc/register.yml file.

In addition, you can generate random data for the three-way control with the scipts `./small_data.sh` or `./big_data.sh`

Now try visiting http://localhost:3000 to see the example Grafana dashboard (credentials admin/admin).

## Note
You can run steward, jetwash and celerity as stand-alone command-line binaries as an alternative. Simply provide the help argument to see more details. For exampple: -

```bash
./jetwash --help
```