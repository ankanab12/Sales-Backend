from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import os

app = Flask(__name__)
CORS(app)

# --- MongoDB Setup ---
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["hemraj_Sales_group"]

orders_col = db["orders"]
dispatch_col = db["dispatches"]
burdwan_col = db["burdwan_stock"]
katwa_col = db["katwa_stock"]
oilorders_col = db["oilorders"]
oildispatch_col = db["oildispatches"] 


def safe_float(value):
    """Convert to float safely (handles None or empty string)."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


# --- Helper: format date to string ---
def format_date(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    return value or ""



def serialize_order(d):
    return {
        "id": d.get("orderId", ""),
        "orderId": d.get("orderId", ""),
        "orderDate": format_date(d.get("orderDate", "")),
        "customerType": d.get("customerType", ""),
        "brokerName": d.get("brokerName", ""),
        "customerName": d.get("customerName", ""),
        "product": d.get("product", ""),
        "riceType": d.get("riceType", ""),
        "riceClass": d.get("riceClass", ""),
        "riceName": d.get("riceName", ""),
        "packaging": d.get("packaging", ""),
        "weight": float(d.get("weight", 0)),
        "bagConfig": int(d.get("bagConfig", 0)),
        "quantity": float(d.get("quantity", 0)),
        "rate": float(d.get("rate", 0)),
        "cost": float(d.get("cost", 0)),
        "gstPercent": float(d.get("gstPercent", 0)),
        "amount": float(d.get("amount", 0)),
        "status": d.get("status", ""),
        "cancelReason": d.get("cancelReason", "")
    }

def serialize_dispatch(d):
    return {
        "id": str(d["_id"]),
        "batchNo": d.get("batchNo", ""),
        "dispatchDate": format_date(d.get("date", "")),         # ✅ Match frontend
        "dueDate": format_date(d.get("dueDate", "")),
        "dispatchLocation": d.get("location", ""),              # ✅ Match frontend
        "customerType": d.get("customerType", ""),
        "brokerName": d.get("brokerName", ""),
        "customerName": d.get("customerName", ""),
        "hsnCode": d.get("hsnCode", ""),                        # ✅ Added properly
        "barCode": d.get("barCode", ""),                        # ✅ Added properly
        "skuCode": d.get("skuCode", ""),                        # ✅ Added properly
        "product": d.get("product", ""),
        "riceType": d.get("riceType", ""),
        "riceClass": d.get("riceClass", ""),
        "riceName": d.get("riceName", ""),
        "packagingType": d.get("packaging", ""),                # ✅ Match frontend key
        "weightKg": d.get("weight", ""),                        # ✅ Match frontend key
        "bagConfig": d.get("bagConfig", ""),
        "quantity": d.get("quantity", ""),
        "rate": d.get("rate", ""),
        "cost": d.get("cost", ""),
        "gstPercent": d.get("gstPercent", ""),
        "gstAmount": d.get("gstAmount", ""),                    # ✅ Added properly
        "amount": d.get("amount", ""),
        "loadingLocation": d.get("loadingLocation", ""),
        "loadingMan": d.get("loadingMan", ""),
        "challanNo": d.get("challan", ""),                      # ✅ Match frontend key
        "challanPhoto": d.get("challanPhoto", ""),
        "carNo": d.get("carNo", ""),
        "carPhoto": d.get("carPhoto", ""),
        "advance": d.get("advance", ""),
        "due": d.get("due", ""),
        "netWeight": d.get("netWeight", ""),
        "driverContact": d.get("driverContact", "")
    }




def serialize_burdwan(record):
    return {
        "id": str(record["_id"]),
        "date": format_date(record.get("date", "")),
        "variant": record.get("variant", ""),
        "brand": record.get("brand", ""),
        "riceType": record.get("riceType", ""),
        "riceName": record.get("riceName", ""),
        "quantity": float(record.get("quantity", 0)),
        "kgPerBag": float(record.get("kgPerBag", 0)),
        "ton": float(record.get("ton", 0))
    }


def serialize_katwa(s):
    return {
        "id": str(s["_id"]),
        "date": format_date(s.get("date", "")),
        "riceType": s.get("riceType", ""),
        "variety": s.get("variety", ""),
        "kari": float(s.get("kari", 0)),
        "godown": float(s.get("godown", 0)),
        "total": float(s.get("total", 0))
    }






def serialize_oilorder(d):
    return {
        "id": str(d["_id"]),
        "batchno": d.get("batchno", ""),
        "orderDate": format_date(d.get("orderDate", "")),
        "customerType": d.get("customerType", ""),
        "brokerName": d.get("brokerName", ""),
        "customerName": d.get("customerName", ""),
        "oilVariant": d.get("oilVariant", ""),
        "brand": d.get("brand", ""),
        "packagingType": d.get("packagingType", ""),
        "weight": float(d.get("weight", 0)),
        "bagConfig": int(d.get("bagConfig", 0)),
        "quantity": float(d.get("quantity", 0)),
        "rate": float(d.get("rate", 0)),
        "cost": float(d.get("cost", 0)),
        "gst": float(d.get("gst", 0)),
        "gstAmount": float(d.get("gstAmount", 0)),
        "amount": float(d.get("amount", 0)),
        "status": d.get("status", ""),
        "cancelReason": d.get("cancelReason", "")
    }


# Serializer for Oil Dispatch
def serialize_oildispatch(d):
    return {
        "id": str(d["_id"]),
        "batchNo": d.get("batchNo", ""),
        "date": format_date(d.get("date", "")),
        "dueDate": format_date(d.get("dueDate", "")),
        "location": d.get("location", ""),
        "customerType": d.get("customerType", ""),
        "brokerName": d.get("brokerName", ""),
        "customerName": d.get("customerName", ""),
        "hsnCode": float(d.get("hsnCode", 0)),
        "barCode": float(d.get("barCode", 0)),
        "skuCode": d.get("skuCode", ""),
        "oilVariant": d.get("oilVariant", ""),
        "brand": d.get("brand", ""),
        "packagingType": d.get("packagingType", ""),
        "weight": float(d.get("weight", 0)),
        "bagConfig": int(d.get("bagConfig", 0)),
        "quantity": float(d.get("quantity", 0)),
        "rate": float(d.get("rate", 0)),
        "cost": float(d.get("cost", 0)),
        "gstPercent": float(d.get("gstPercent", 0)),
        "amount": float(d.get("amount", 0)),
        "advance": float(d.get("advance", 0)),
        "due": float(d.get("due", 0)),
        "loadingLocation": d.get("loadingLocation", ""),
        "loadingMan": d.get("loadingMan", ""),
        "challan": d.get("challan", ""),
        "challanPhoto": d.get("challanPhoto", ""),
        "carNo": d.get("carNo", ""),
        "carPhoto": d.get("carPhoto", ""),
        "netWeight": d.get("netWeight", ""),
        "driverContact": d.get("driverContact", "")
    }


# ==============================================================
# RICE ORDERS CRUD
# ==============================================================

@app.route("/orders", methods=["GET"])
def get_orders():
    return jsonify([serialize_order(o) for o in orders_col.find().sort("_id", -1)])


@app.route("/orders", methods=["POST"])
def add_order():
    data = request.json

    # ✅ Ensure custom orderId is used
    data["orderId"] = data.get("id") or data.get("orderId")  # accept frontend id as orderId
    if not data["orderId"]:
        # fallback: generate server-side ID
        now = datetime.now()
        data["orderId"] = f"ORD-{now.strftime('%Y%m%d-%H%M%S')}"

    # clean up
    data.pop("id", None)

    # ✅ Ensure proper date and numeric conversions
    data["orderDate"] = format_date(data.get("orderDate", datetime.now()))
    numeric_fields = ["weight", "bagConfig", "quantity", "rate", "cost", "gstPercent", "amount"]
    for field in numeric_fields:
        data[field] = float(data.get(field, 0))

    # insert into MongoDB
    orders_col.insert_one(data)
    return jsonify({"message": "Order added successfully"}), 201



@app.route("/orders/<oid>", methods=["PUT"])
def update_order(oid):
    data = request.json
    data["orderDate"] = format_date(data.get("orderDate", datetime.now()))
    numeric_fields = ["weight", "bagConfig", "quantity", "rate", "cost", "gstPercent", "amount"]
    for field in numeric_fields:
        data[field] = float(data.get(field, 0))

    result = orders_col.update_one({"orderId": oid}, {"$set": data})
    if result.matched_count == 0:
        return jsonify({"error": "No order found for given orderId"}), 404

    return jsonify({"message": "Order updated successfully"})



@app.route("/orders/<oid>", methods=["DELETE"])
def delete_order(oid):
    result = orders_col.delete_one({"orderId": oid})
    if result.deleted_count == 0:
        return jsonify({"error": "No order found for given orderId"}), 404
    return jsonify({"message": "Order deleted successfully"})




# ==============================================================
# ✅ RICE DISPATCH CRUD
# ==============================================================

@app.route("/dispatches", methods=["GET"])
def get_dispatches():
    try:
        records = [serialize_dispatch(d) for d in dispatch_col.find().sort("_id", -1)]
        return jsonify(records)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/dispatches", methods=["POST"])
def add_dispatch():
    data = request.json
    try:
        data["batchNo"] = data.get("batchNo") or f"DIS-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        data["date"] = format_date(data.get("dispatchDate", datetime.now()))
        data["dueDate"] = format_date(data.get("dueDate", ""))
        data["location"] = data.get("dispatchLocation", "")
        data["packaging"] = data.get("packagingType", "")
        data["weight"] = safe_float(data.get("weightKg"))
        data["gstAmount"] = safe_float(data.get("gstAmount"))
        data["challan"] = data.get("challanNo", "")

        numeric_fields = [
            "weight", "bagConfig", "quantity", "rate", "cost",
            "gstPercent", "gstAmount", "amount", "advance", "due"
        ]
        for field in numeric_fields:
            data[field] = safe_float(data.get(field))

        result = dispatch_col.insert_one(data)
        inserted = dispatch_col.find_one({"_id": result.inserted_id})
        return jsonify(serialize_dispatch(inserted)), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/dispatches/<did>", methods=["PUT"])
def update_dispatch(did):
    data = request.json
    try:
        data["date"] = format_date(data.get("dispatchDate", datetime.now()))
        data["dueDate"] = format_date(data.get("dueDate", ""))
        data["location"] = data.get("dispatchLocation", "")
        data["packaging"] = data.get("packagingType", "")
        data["weight"] = safe_float(data.get("weightKg"))
        data["gstAmount"] = safe_float(data.get("gstAmount"))
        data["challan"] = data.get("challanNo", "")

        numeric_fields = [
            "weight", "bagConfig", "quantity", "rate", "cost",
            "gstPercent", "gstAmount", "amount", "advance", "due"
        ]
        for field in numeric_fields:
            data[field] = safe_float(data.get(field))

        result = dispatch_col.update_one({"_id": ObjectId(did)}, {"$set": data})
        if result.matched_count == 0:
            return jsonify({"error": "No dispatch found for given ID"}), 404

        updated = dispatch_col.find_one({"_id": ObjectId(did)})
        return jsonify(serialize_dispatch(updated))
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/dispatches/<did>", methods=["DELETE"])
def delete_dispatch(did):
    try:
        result = dispatch_col.delete_one({"_id": ObjectId(did)})
        if result.deleted_count == 0:
            return jsonify({"error": "No dispatch found for given ID"}), 404
        return jsonify({"message": "Dispatch deleted successfully"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# ---------------- Burdwan Stock ----------------
@app.route("/burdwan_stock", methods=["GET"])
def get_burdwan_stock():
    stocks = [serialize_burdwan(s) for s in burdwan_col.find().sort("_id", -1)]
    return jsonify(stocks)


@app.route("/burdwan_stock", methods=["POST"])
def add_burdwan_stock():
    data = request.json
    data["date"] = format_date(data.get("date", datetime.now()))
    for field in ["quantity", "kgPerBag", "ton"]:
        data[field] = float(data.get(field, 0))
    burdwan_col.insert_one(data)
    return jsonify({"message": "Stock added successfully"}), 201


@app.route("/burdwan_stock/<sid>", methods=["PUT"])
def update_burdwan_stock(sid):
    data = request.json
    data["date"] = format_date(data.get("date", datetime.now()))
    for field in ["quantity", "kgPerBag", "ton"]:
        data[field] = float(data.get(field, 0))
    burdwan_col.update_one({"_id": ObjectId(sid)}, {"$set": data})
    return jsonify({"message": "Stock updated successfully"})


@app.route("/burdwan_stock/<sid>", methods=["DELETE"])
def delete_burdwan_stock(sid):
    burdwan_col.delete_one({"_id": ObjectId(sid)})
    return jsonify({"message": "Stock deleted successfully"})


# ---------------- Katwa Stock ----------------
@app.route("/katwa_stock", methods=["GET"])
def get_katwa_stock():
    stocks = [serialize_katwa(s) for s in katwa_col.find().sort("_id", -1)]
    return jsonify(stocks)


@app.route("/katwa_stock", methods=["POST"])
def add_katwa_stock():
    data = request.json
    data["date"] = format_date(data.get("date", datetime.now()))
    for field in ["kari", "godown", "total"]:
        data[field] = float(data.get(field, 0))
    katwa_col.insert_one(data)
    return jsonify({"message": "Katwa stock added successfully"}), 201


@app.route("/katwa_stock/<sid>", methods=["PUT"])
def update_katwa_stock(sid):
    data = request.json
    data["date"] = format_date(data.get("date", datetime.now()))
    for field in ["kari", "godown", "total"]:
        data[field] = float(data.get(field, 0))
    katwa_col.update_one({"_id": ObjectId(sid)}, {"$set": data})
    return jsonify({"message": "Katwa stock updated successfully"})


@app.route("/katwa_stock/<sid>", methods=["DELETE"])
def delete_katwa_stock(sid):
    katwa_col.delete_one({"_id": ObjectId(sid)})
    return jsonify({"message": "Katwa stock deleted successfully"})






# ---------------- Oil Orders CRUD ----------------
@app.route("/oilorders", methods=["GET"])
def get_oilorders():
    return jsonify([serialize_oilorder(d) for d in oilorders_col.find().sort("_id",-1)])


@app.route("/oilorders", methods=["POST"])
def add_oilorder():
    data = request.json
    data["orderDate"]=format_date(data.get("orderDate",datetime.now()))
    numeric_fields=["weight","bagConfig","quantity","rate","cost","gst","gstAmount","amount"]
    for field in numeric_fields:
        data[field] = float(data.get(field,0))
    oilorders_col.insert_one(data)
    return jsonify({"message":"Oil order added successfully"}),201


@app.route("/oilorders/<oid>", methods=["PUT"])
def update_oilorder(oid):
    data = request.json
    data["orderDate"]=format_date(data.get("orderDate",datetime.now()))
    numeric_fields=["weight","bagConfig","quantity","rate","cost","gst","gstAmount","amount"]
    for field in numeric_fields:
        data[field] = float(data.get(field,0))
    oilorders_col.update_one({"_id":ObjectId(oid)},{"$set":data})
    return jsonify({"message":"Oil order updated successfully"})


@app.route("/oilorders/<oid>", methods=["DELETE"])
def delete_oilorder(oid):
    result = oilorders_col.delete_one({"_id":ObjectId(oid)})
    if result.deleted_count==0:
        return jsonify({"error":"No oil order found for given ID"}),404
    return jsonify({"message":"Oil order deleted successfully"})




# GET all oil dispatches
@app.route("/oildispatches", methods=["GET"])
def get_oildispatches():
    try:
        records = [serialize_oildispatch(d) for d in oildispatch_col.find().sort("_id", -1)]
        return jsonify(records)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# POST a new oil dispatch
@app.route("/oildispatches", methods=["POST"])
def add_oildispatch():
    data = request.json
    try:
        data["date"] = format_date(data.get("date", datetime.now()))
        data["dueDate"] = format_date(data.get("dueDate", ""))

        numeric_fields = [
            "hsnCode", "barCode", "weight", "bagConfig", "quantity", "rate", "cost",
            "gstPercent", "gstAmount", "amount", "advance", "due"
        ]
        for field in numeric_fields:
            data[field] = float(data.get(field, 0))

        result = oildispatch_col.insert_one(data)
        inserted = oildispatch_col.find_one({"_id": result.inserted_id})
        return jsonify(serialize_oildispatch(inserted)), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# PUT update oil dispatch by ID
@app.route("/oildispatches/<did>", methods=["PUT"])
def update_oildispatch(did):
    data = request.json
    try:
        data["date"] = format_date(data.get("date", datetime.now()))
        data["dueDate"] = format_date(data.get("dueDate", ""))

        numeric_fields = [
            "hsnCode", "barCode", "weight", "bagConfig", "quantity", "rate", "cost",
            "gstPercent", "gstAmount","amount", "advance", "due"
        ]
        for field in numeric_fields:
            data[field] = float(data.get(field, 0))

        result = oildispatch_col.update_one({"_id": ObjectId(did)}, {"$set": data})
        if result.matched_count == 0:
            return jsonify({"error": "No oil dispatch found for given ID"}), 404

        updated = oildispatch_col.find_one({"_id": ObjectId(did)})
        return jsonify(serialize_oildispatch(updated))
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# DELETE oil dispatch by ID
@app.route("/oildispatches/<did>", methods=["DELETE"])
def delete_oildispatch(did):
    try:
        result = oildispatch_col.delete_one({"_id": ObjectId(did)})
        if result.deleted_count == 0:
            return jsonify({"error": "No oil dispatch found for given ID"}), 404
        return jsonify({"message": "Oil dispatch deleted successfully"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400





if __name__ == "__main__":
    app.run(debug=True)