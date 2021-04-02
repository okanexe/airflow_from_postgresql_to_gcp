from google.cloud import storage

bucket_name = "okans_database"
source_file_name = "/Users/okans/Desktop/deneme.csv"
destination_blob_name = "deneme.csv"

storage_client = storage.Client.from_service_account_json('/Users/okans/Desktop/credit.json')
#storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)

blob.upload_from_filename(source_file_name)

print(
    "File {} uploaded to {}.".format(
        source_file_name, destination_blob_name
    )
)

# storage'a data atıldıktan sonra tabloya tarih ekle. sorgu atarken tablodaki son tarih + datetime.now() yapılacak kayıp
# veri kaçırılmayacak.
#connect'te yaptığım gibi bu safhaları transaction olarak yap elektrik kesimi ve ya herhangi hatada hareket edilmesin.


# dag dosyasının içinde csv'de tarihler tutularak da sorguda tarih ayarlaması yapılabilir.