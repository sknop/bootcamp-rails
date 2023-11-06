# Schedule

https://wiki.openraildata.com/index.php?title=SCHEDULE

## CIF Format
https://wiki.openraildata.com/index.php?title=CIF_File_Format

curl -L -u "${TF_VAR_nrod_username}:${TF_VAR_nrod_password}" -o file.cif.gz 'https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full.CIF.gz'

## JSON Format

curl -L -u "${TF_VAR_nrod_username}:${TF_VAR_nrod_password}" -o file.json.gz 'https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full'


Field	Description
transaction_type	create, update or delete. A full snapshot file will only contain create records. Daily updates may contain either.

tiploc_code	TIPLOC of the location
nalco	NLC (National Location Code)
stanox	STANOX (Station Number)
crs_code	CRS (Computer Reservation System) code. Now 3-Alpha Code.
description	The short name of the location. Not populated for every location.
tps_description	The name of the location

JsonTimetableV1

JsonAssociationV1
JsonScheduleV1
TiplocV1
