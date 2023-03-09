#!/bin/bash

function proxies() {
    toxiproxy-cli list
}

function proxyclean() {
    toxiproxy-cli delete toxic-pub
    toxiproxy-cli delete toxic-sub
}

function slowdownsub() {
  toxiproxy-cli toxic add -t bandwidth -toxicName slowdownsub -a rate=0 --downstream toxic-sub 
}

function slowupsub() {
  toxiproxy-cli toxic add -t bandwidth -toxicName slowupsub -a rate=0 --upstream toxic-sub
}

function slowupsubclean {
  toxiproxy-cli toxic delete -toxicName slowupsub toxic-sub
}

function slowdownsubclean {
  toxiproxy-cli toxic delete -toxicName slowdownsub toxic-sub
}

function proxy() {
  toxiproxy-cli create --listen localhost:1883 --upstream beamd:1883 toxic-pub
  toxiproxy-cli create --listen localhost:1884 --upstream beamd:1883 toxic-sub
}

"$@"
