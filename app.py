from flask import Flask, request, jsonify
import uuid, datetime, json, os
from google.cloud import bigquery

app = Flask(__name__)
bq = bigquery.Client()
PROJECT = os.environ.get("PROJECT")

def uuidv7():
    # Use proper UUID generation with domain prefix
     # Cheap stand-in; use a proper uuid7 lib in prod
    base_uuid = str(uuid.uuid4()).replace('-', '').upper()
    return base_uuid

TABLES = {
    "supplier": f"{PROJECT}.core_idmaps.id_map_supplier",
    "material": f"{PROJECT}.core_idmaps.id_map_material",
    "location": f"{PROJECT}.core_idmaps.id_map_location",
}

@app.post("/id")
def mint():
    """
    Request JSON:
      { "domain":"supplier|material|location", "source_system":"SAP", "source_id":"00012345" }
    Response JSON:
      { "pk":"SUP_001_01HK2B3C4D5E6F7G8H", "domain":"supplier" }
    """
    body = request.get_json()
    domain = body["domain"]
    source_system = body["source_system"]
    source_id = body["source_id"]

    table = TABLES[domain]
    # FIXED: Added domain prefix to enterprise key
    domain_prefix = {"supplier": "SUP", "material": "MAT", "location": "LOC"}[domain]
    pk = f"{domain_prefix}_{uuidv7()}"
    
    row = {
        f"{domain}_pk": pk,
        "source_system": source_system,
        "source_id": source_id,
        "is_active": True,
        "created_ts": datetime.datetime.utcnow().isoformat(),
        "updated_ts": datetime.datetime.utcnow().isoformat(),
        "metadata": {"source": "id_service"}
    }
    bq.insert_rows_json(table, [row])  # simple insert
    return jsonify({"pk": pk, "domain": domain})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
