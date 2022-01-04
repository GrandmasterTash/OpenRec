# celerity
A blazingly fast reconciliation server written in Rust with an emphasis on low system resource usage regardless of data volume.

TODO: Abort-type changeset.
TODO: Examples at top-level should be Jetwash+celerity - consider moving integration tests up from celerity to top-level and incorporate both jetwash and celeiity.
TODO: Update and full refresh data loads.
TODO: folder structure docs....
TODO: archive/celerity
TODO: archive/jetwash folders - ditch /original folder.
TODO: Consider a 'watch tree ./tmp -h' - style progress meter showing files and sizes.


## Experimentation around a Rust reconcilliation engine and gRPC server.
TODO: Document child projects in this file.
TODO: Sentinal to monitor inbox and rename .ready
TODO: Sentinal to initiate match jobs  (jetwash then celerity)
TODO: Sentinal to publish unmatched data to outbox
TODO: Sentinal to publish match job stats to prometheus.