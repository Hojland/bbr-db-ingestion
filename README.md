# BBR ingestion service
This is a service that ingest BBR dat from datafordeler.dk


# Dokumentation for BBR 
https://confluence.datafordeler.dk/pages/viewpage.action?pageId=16056582

# All URLs for BBR
bbr_bygning_url = "https://services.datafordeler.dk//BBR/BBRPublic/1/REST/bygning?"
bbr_enhed_url = "https://services.datafordeler.dk//BBR/BBRPublic/1/REST/enhed?"
bbr_ejendomsrelation_url = "https://services.datafordeler.dk//BBR/BBRPublic/1/REST/ejendomsrelation?"
br_sag_url = "https://services.datafordeler.dk//BBR/BBRPublic/1/REST/bbrsag?"
bbr_grund_url = "https://services.datafordeler.dk//BBR/BBRPublic/1/REST/grund?"
bbr_tekniskanlaeg_url = "https://services.datafordeler.dk//BBR/BBRPublic/1/REST/tekniskanlaeg?"

# Pull hændelser BBR
"https://services.datafordeler.dk/system/EventMessages/1.0.0/custom?datefrom=2020-01-01&dateto=2020-02-01&username=<some_username>&password=<some_password>&format=Json&page=1&pagesize=1000"
statuscode: https://teknik.bbr.dk/kodelister/0/1/0/Livscyklus


# TODO
0. read a lot on BBR and how it works on datafordeler
01. add all the metadata files we want, and store them in s3 bucket
1. change to push implementation to endpoint using serverless
2. figure out how to make service to switch codes to actual values from https://teknik.bbr.dk/kodelister/0/1/0
3. figure out how often there are different status_codes and how we should respond differently to all of them? We shouldn't update date_to if it was wrongly registered etc
4. make indexes when making whole new stuff


# NOTICE
Id in pull hændelses is the id_lokalId variable