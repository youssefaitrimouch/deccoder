from random import randint
from datetime import datetime
from base64 import b64decode
from os import makedirs, unlink, path as Path
import glob
from utils import *
from elasticsearch import Elasticsearch
# from urllib3 import disable_warnings
# disable_warnings()

TEMP_FOLDER = "/mnt/data/cdr/raw"
DECODED_FOLDER = "/mnt/data/cdr/decoded"
ES_INDEX = "decoded_cdr"

# decode base64 to string
def decode(string):
    return b64decode(string.encode('utf-8')).decode('utf-8')

if __name__ == "__main__":
    # to index data in elasticsearch
    es = Elasticsearch(['http://elasticsearch.monitoring.svc.cluster.local:9200'])
#     es = Elasticsearch(
#     ['https://elasticsearch-master.monitoring.svc.cluster.local:9200'],
#     http_auth=('elastic', 'ArNMA3e7ct0fuvdT'),
#     verify_certs=False
# )
    # es = Elasticsearch(['http://elasticsearch-master-headless.default.svc.cluster.local:9200'],http_auth=('elastic', 'hpTxdiShRYlyXrMt'))
    # es=Elasticsearch([{"host":"elasticsearch-master-headless.default.svc.cluster.local","port":9200}],http_auth=('elastic', 'slzIsnsUD2hicTPF'))

    print(f"{datetime.now()} - Start decoding raw data for date : {datetime.now().strftime('%Y-%m-%d.%H')}...")
    current = datetime.now().strftime('%Y-%m-%d  %H')
    date = current.split("  ")[0]
    heure = current.split("  ")[1]

    # get all raw files : 
    files = glob.glob(TEMP_FOLDER + "/*/*.csv")

    # read each file, decode base64 and save it to a new file in the DECODED_FOLDER folder
    for file in files:
        print(f"\n{datetime.now()} - Decoding {file}...")

        # var to contain the decoded csv
        decoded = ""

        # read file
        with open(file, 'r', encoding="utf(8") as f:
            lines = f.readlines()
        numlines = len(lines)
        decoded = []
        for line in lines:
            decoded.append(decode(line))
        
        # create the new file
        new_file = file.replace(TEMP_FOLDER, DECODED_FOLDER)+".done"

        # create the folder if it doesn't exist
        makedirs(Path.dirname(new_file), exist_ok=True)

        # je veux que ~10% des fichiers soient corrompus : nombre de lignes dans le fichier DECODE est different du nombre de lignes dans le fichier RAW
        to_delete = 0
        if randint(0, 10) == 0:
            # dans ce cas, on enleve entre 500 et 1000 lignes :
            to_delete = randint(500, 1000)
            decoded = "\n".join(decoded[:-to_delete])
            print(f"\n{datetime.now()} - File is corrupted, could not recover {to_delete} lines...\n")
        else:
            decoded = "\n".join(decoded)

             
        with open(new_file, "w", encoding="utf-8") as f:
            f.write(decoded)
        print(f"{datetime.now()} - File {new_file} created.")

        # delete the raw file
        print(f"{datetime.now()} - Deleting {file}...")
        try:
            unlink(file)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file, e))
        
        print(f"{datetime.now()} - File {file} deleted.")

        print(f"{datetime.now()} - Indexing into elasticsearch")
        folder = file.replace("\\", "/").split("/")[-2]
        flux =  [d["flux"] for d in data if d["chemin"] == folder][0]
        techno =  [d["techno"] for d in data if d["chemin"] == folder][0]
        filename = new_file.replace("\\", "/").split("/")[-1].replace(".done", "")
        doc = {
            "type": "DECODED",
            "date": date,
            "heure": heure,
            "count": numlines - to_delete -1,
            "flux": flux,
            "techno": techno,
            "path": filename,
            "executed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }


        resp = es.index(index=ES_INDEX, document=doc)

        print(f"{datetime.now()} - Indexing into elasticsearch done. {resp['result']}")
        

    

    print(f"\n{datetime.now()} - Finished Decoding CDRs.")
    

    print("\n ---------------------------------------------------------------------------------- \n")


# from random import randint
# from datetime import datetime
# from base64 import b64decode
# from os import makedirs, unlink, path as Path
# import glob
# # from utils import *

# TEMP_FOLDER = "/mnt/data/cdr/raw"
# DECODED_FOLDER = "/mnt/data/cdr/decoded"

# # TEMP_FOLDER = "C:/Users/HP/Desktop/Volume/CDR/raw"
# # DECODED_FOLDER = "C:/Users/HP/Desktop/Volume/CDR/decoded"

# # decode base64 to string
# def decode(string):
#     return b64decode(string.encode('utf-8')).decode('utf-8')

# data = [
#     {
#         "flux": "MSS",
#         "techno": "Ericsson",
#         "chemin": "Ericsson",
#         "table": "ericsson_tbl",
#     },
#     {
#         "flux": "MSS",
#         "techno": "Huawei",
#         "chemin": "MSC_Huawei",
#         "table": "huawei_tbl",
#     },
#     {
#         "flux": "MSS",
#         "techno": "Nokia",
#         "chemin": "NSN",
#         "table": "nokia_tbl",
#     },
#     {
#         "flux": "OCS",
#         "techno": "REC",
#         "chemin": "OCSREC",
#         "table": "ocsrec_tbl",
#     },
#     {
#         "flux": "OCS",
#         "techno": "VOU",
#         "chemin": "OCSVOU",
#         "table": "ocsvou_tbl",
#     },
#     {
#         "flux": "OCS",
#         "techno": "SMS",
#         "chemin": "OCSSMS",
#         "table": "ocssms_tbl",
#     },
#     {
#         "flux": "DATA Huawei",
#         "techno": "PGW",
#         "chemin": "PGW3",
#         "table": "pgw_tbl",
#     },
#     {
#         "flux": "DATA Huawei",
#         "techno": "SGW",
#         "chemin": "4G_SGW",
#         "table": "sgw_tbl",
#     },
#     {
#         "flux": "DATA Ericsson",
#         "techno": "PGW",
#         "chemin": "PGW_Ericsson",
#         "table": "epgw_tbl",
#     },
#     {
#         "flux": "DATA Ericsson",
#         "techno": "SGW",
#         "chemin": "SGW_Ericsson",
#         "table": "esgw_tbl",
#     },
#     {
#         "flux": "DATA Ericsson",
#         "techno": "SGSN",
#         "chemin": "SGSN_Ericsson",
#         "table": "rsgsn_tbl",
#     },
#     {
#         "flux": "FIXE",
#         "techno": "Alcatel",
#         "chemin": "Alcatel",
#         "table": "alcatel_tbl",
#     },
#     {
#         "flux": "FIXE",
#         "techno": "IMS_NOKIA",
#         "chemin": "IMS",
#         "table": "ims_tbl",
#     },
#     {
#         "flux": "FIXE",
#         "techno": "MMP",
#         "chemin": "MMP",
#         "table": "mmp_tbl",
#     },
#     {
#         "flux": "FIXE",
#         "techno": "Siemens",
#         "chemin": "Siemens",
#         "table": "siemens_tbl",
#     },
#     {
#         "flux": "FIXE",
#         "techno": "TSSA",
#         "chemin": "TSSA",
#         "table": "tssa_tbl",
#     },
#     {
#         "flux": "FIXE",
#         "techno": "TSSI",
#         "chemin": "TSSI",
#         "table": "fixe_tssi",
#     }
# ]

# if __name__ == "__main__":
#     # to index data in elasticsearch

#     print(f"{datetime.now()} - Start decoding raw data for date : {datetime.now().strftime('%Y-%m-%d.%H')}...")
#     current = datetime.now().strftime('%Y-%m-%d  %H')
#     date = current.split("  ")[0]
#     heure = current.split("  ")[1]
    
#     # get all raw files : 
#     files = glob.glob(TEMP_FOLDER + "/*/*.csv")

#     # read each file, decode base64 and save it to a new file in the DECODED_FOLDER folder
#     for file in files:
#         print(f"\n{datetime.now()} - Decoding {file}...")

#         # var to contain the decoded csv
#         decoded = ""

#         # read file
#         with open(file, 'r', encoding="utf(8") as f:
#             lines = f.readlines()
#         numlines = len(lines)
#         decoded = []
#         for line in lines:
#             decoded.append(decode(line))
        
#         # create the new file
#         new_file = file.replace(TEMP_FOLDER, DECODED_FOLDER)+".done"

#         # create the folder if it doesn't exist
#         makedirs(Path.dirname(new_file), exist_ok=True)

#         # je veux que ~10% des fichiers soient corrompus : nombre de lignes dans le fichier DECODE est different du nombre de lignes dans le fichier RAW
#         to_delete = 0
#         if randint(0, 10) == 0:
#             # dans ce cas, on enleve entre 500 et 1000 lignes :
#             to_delete = randint(500, 1000)
#             decoded = "\n".join(decoded[:-to_delete])
#             print(f"\n{datetime.now()} - File is corrupted, could not recover {to_delete} lines...\n")
#         else:
#             decoded = "\n".join(decoded)

#         # write the decoded csv to the new file
#         with open(new_file, "w", encoding="utf-8") as f:
#             f.write(decoded)
#         print(f"{datetime.now()} - File {new_file} created.")

#         # delete the raw file
#         print(f"{datetime.now()} - Deleting {file}...")
#         try:
#             unlink(file)
#         except Exception as e:
#             print('Failed to delete %s. Reason: %s' % (file, e))
        
#         print(f"{datetime.now()} - File {file} deleted.")

#         print(f"{datetime.now()} - Indexing into elasticsearch")
#         folder = file.replace("\\", "/").split("/")[-2]
#         flux =  [d["flux"] for d in data if d["chemin"] == folder][0]
#         techno =  [d["techno"] for d in data if d["chemin"] == folder][0]
#         filename = new_file.replace("\\", "/").split("/")[-1].replace(".done", "")
#         doc = {
#             "type": "DECODED",
#             "date": date,
#             "heure": heure,
#             "count": numlines - to_delete -1,
#             "flux": flux,
#             "techno": techno,
#             "path": filename,
#             "executed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#         }
       
       


    

#     print(f"\n{datetime.now()} - Finished Decoding CDRs.")
    

#     print("\n ---------------------------------------------------------------------------------- \n")

