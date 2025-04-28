
import os, pathlib, textwrap
import pandas as pd
import plotly.express as px
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
from sqlalchemy import create_engine

POSTGRES_DSN = "" 

BASE = pathlib.Path(__file__).parent
DATA_DIR = BASE / "data"

def load_or_query(table, sql=None):
    csv_path = DATA_DIR / f"{table}.csv"
    if csv_path.exists():
        return pd.read_csv(csv_path)
    elif POSTGRES_DSN and sql:
        engine = create_engine(POSTGRES_DSN)
        with engine.connect() as con:
            return pd.read_sql(sql, con)
    else:
        raise FileNotFoundError(f"Need {table}.csv or a live DSN")

customers = load_or_query(
    "Customers",
    "SELECT cust_id, first_name, last_name, salary_bracket FROM Customers",
)
accounts = load_or_query(
    "Accounts",
    "SELECT account_no, cust_id, balance FROM Accounts",
)
transactions = load_or_query(
    "Transactions",
    textwrap.dedent("""
        SELECT tid, t_type, amount, time::date AS day,
               source_acc_no, dest_acc_no
        FROM Transactions;
    """),
)
creditcards = load_or_query(
    "CreditCards",
    "SELECT cust_id, outstanding FROM CreditCards",
)
sessions = load_or_query(
    "Sessions",
    "SELECT cust_id, session_id FROM Sessions",
)

customers["customer"] = customers["first_name"] + " " + customers["last_name"]

txn_by_type = (transactions
               .groupby("t_type", as_index=False)
               .agg(txn_count=("tid","size"),
                    total_amount=("amount","sum")))

top_cardholders = (creditcards
                   .groupby("cust_id", as_index=False)
                   .agg(total_outstanding=("outstanding","sum"))
                   .merge(customers[["cust_id","customer"]])
                   .nlargest(5, "total_outstanding"))

multi_session = (sessions
                 .groupby("cust_id", as_index=False)
                 .agg(session_count=("session_id","size"))
                 .query("session_count > 1")
                 .merge(customers[["cust_id","customer"]]))

dormant = (accounts
           .merge(transactions[["source_acc_no","dest_acc_no"]]
                  .melt(var_name="role", value_name="account_no"),
                  how="left", indicator=True)
           .query("_merge == 'left_only'"))

last_txn = (transactions
            .merge(accounts[["account_no","cust_id"]]
                   .rename(columns={"account_no":"source_acc_no"}),
                   how="left")
            .groupby("cust_id", as_index=False)
            .agg(last_time=("day","max"))
            .merge(customers[["cust_id","customer"]]))

fig_txn_mix = px.bar(
    txn_by_type, x="t_type", y="txn_count",
    text="txn_count", title="Transaction Mix by Type",
)
fig_txn_mix.update_traces(textposition="outside")

fig_top_cards = px.bar(
    top_cardholders.sort_values("total_outstanding"),
    x="total_outstanding", y="customer", orientation="h",
    title="Top 5 Customers by Credit-Card Outstanding",
)
fig_top_cards.update_layout(yaxis_title="Customer", xaxis_title="Outstanding ($)")

fig_sessions = px.bar(
    multi_session.sort_values("session_count"),
    x="session_count", y="customer", orientation="h",
    title="Customers with Multiple Sessions",
)

fig_dormant = px.scatter(
    dormant, x="account_no", y="balance", title="Dormant Accounts Holding Funds",
    labels={"account_no":"Account #", "balance":"Balance ($)"},
)

fig_last_txn = px.histogram(
    last_txn, x="last_time", nbins=30, title="Distribution of Last Transaction Dates",
)

CAPTIONS = {
    "mix":  "Deposits, withdrawals and transfers occur in roughly equal "
            "frequency, confirming our synthetic workload is balanced. "
            "Any future spike in one category could indicate anomalous activity.",
    "top":  "High outstanding balances are concentrated in a handful of customers. "
            "These accounts deserve tighter fraud thresholds and proactive monitoring.",
    "sess": "Users with many distinct login sessions provide rich behavioural baselines. "
            "Sudden changes in their session cadence may point to credential compromise.",
    "dorm": "Several accounts hold significant funds yet have never transacted. "
            "Fraudsters often target such dormant accounts for money-laundering.",
    "last": "The bulk of customers made at least one transaction in the past 45 days. "
            "Long tails in this distribution highlight inactive customers who could be "
            "re-engaged or monitored for sudden large-value transfers.",
}

app = dash.Dash(__name__, title="Fraud DB Dashboard")
app.layout = html.Div(
    style={"fontFamily":"Roboto, sans-serif", "padding":"1rem"},
    children=[
        html.H2("Fraud-Detection Database Dashboard"),
        html.P("Interactive overview of key metrics derived from Phase-2 data."),
        html.Div([
            html.Div([
                dcc.Graph(figure=fig_txn_mix),
                html.P(CAPTIONS["mix"]),
                dcc.Graph(figure=fig_top_cards),
                html.P(CAPTIONS["top"]),
                dcc.Graph(figure=fig_last_txn),
                html.P(CAPTIONS["last"]),
            ], style={"width":"48%","display":"inline-block","verticalAlign":"top"}),
            html.Div([
                dcc.Graph(figure=fig_sessions),
                html.P(CAPTIONS["sess"]),
                dcc.Graph(figure=fig_dormant),
                html.P(CAPTIONS["dorm"]),
            ], style={"width":"48%","display":"inline-block","marginLeft":"4%"}),
        ]),
        html.Hr(),
        html.P("Built with Dash + Plotly • Source SQL in comments • © 2025 S-Cube Team")
    ]
)

if __name__ == "__main__":
    app.run_server(debug=True)
