# SQL Alchemy
import sqlalchemy
from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy.engine import reflection

#import egnine
from config import engine_url

# PyMySQL 
import pymysql
pymysql.install_as_MySQLdb()

# Pandas
import pandas as pd

#flash and jasonify
from flask import Flask, jsonify, make_response


#################################################
# Database Setup
#################################################
engine = create_engine(engine_url)
con = engine.connect()

# Create our session (link) from Python to the DB
# session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/address<br/>"
        f"/api/v1.0/email<br/>"
        f"/api/v1.0/phone<br/>"
    )


@app.route("/api/v1.0/address/<firm_id>")
def address(firm_id):
    # Query firm specific address fields
    sql_address = """ SELECT p.id as person_id, 
        pdata.street, 
        pdata.street_2, pdata.city,
        pdata.state,
        pdata.postal_code,
        pdata.country,
        reg.name as region,
        if(pdata.id = p.preferred_address_id, "preferred","") as preferred
        from `people` as p
        inner join person_addresses as pdata on p.id = pdata.person_id
        left join regions as reg on pdata.`region_id` = reg.id
        left join location_types as loc on pdata.location_type_id = loc.id
        where p.firm_id = %s
        order by person_id """
    address_df = pd.read_sql_query(sql_address, con, params=[firm_id])
    # address_csv = address_df.to_csv("address.csv", sep='\t', encoding='utf-8')
    # address_json = address_df.to_json()

    resp = make_response(address_df.to_csv())
    resp.headers["Content-Disposition"] = "attachment; filename=address.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp

@app.route("/api/v1.0/email/<firm_id>")
def email(firm_id):
    # SQL to split out email
    sql_email = """ SELECT p.id as person_id,
        pdata.`address` as email,
        loc.name as type,
        if(pdata.id = p.preferred_email_address_id, "preferred","") as preferred
        from `people` as p
        inner join person_email_addresses as pdata on p.id = pdata.person_id
        left join location_types as loc on pdata.location_type_id = loc.id
        where p.firm_id = %s
        order by person_id"""
    email_df = pd.read_sql_query(sql_email, con, params=[firm_id])
    # email_csv = email_df.to_csv("email.csv", sep='\t', encoding='utf-8')

    resp = make_response(email_df.to_csv())
    resp.headers["Content-Disposition"] = "attachment; filename=email.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp

@app.route("/api/v1.0/phone/<firm_id>")
def phone(firm_id):
    # SQL to split out email
    sql_phone = """SELECT p.id as person_id,
            pdata.digits as phone,
            pdata.extension as extension,
            loc.name as type,
            if(pdata.id = p.preferred_phone_number_id, "preferred","") as preferred
            from `people` as p
            inner join person_phone_numbers as pdata on p.id = pdata.person_id
            left join location_types as loc on pdata.location_type_id = loc.id
            where p.firm_id = %s
            order by person_id"""
    phone_df = pd.read_sql_query(sql_phone, con, params=[firm_id])
    # phone_df.to_csv("phone.csv", sep='\t', encoding='utf-8')

    resp = make_response(phone_df.to_csv())
    resp.headers["Content-Disposition"] = "attachment; filename=phone.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp

if __name__ == '__main__':
    app.run(debug=True)
