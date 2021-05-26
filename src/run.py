import bitcask

storage = bitcask.BitCask("./bitcask_storage")
storage.put("shivam2", "kumar2 jha")
print(storage.get("shivam2"))