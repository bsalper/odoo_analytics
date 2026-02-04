import json

with open("gcp-key.json", "r", encoding="utf-8") as f:
    data = json.load(f)

print("JSON leído correctamente ✨")
print("Claves encontradas:")
for k in data.keys():
    print("-", k)
