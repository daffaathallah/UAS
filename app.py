from flask import Flask, jsonify
import dask.dataframe as dd
import sqlalchemy

app = Flask(__name__)
DATABASE_URL = "sqlite:///data.db"

@app.route("/list")
def list_data():
    engine = sqlalchemy.create_engine(DATABASE_URL)
    ddf = dd.read_sql_table(
        table_name="device_energy_log",
        con=engine,
        index_col="id",
        npartitions=2
    )
    data = ddf.compute().to_dict(orient="records")
    return jsonify(data)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8123)
