
import os, pathlib, textwrap
import pandas as pd
import plotly.express as px
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
from sqlalchemy import create_engine


BASE = pathlib.Path(__file__).parent
DATA_DIR = BASE / "data"

def load_or_query(table):
    csv_path = DATA_DIR / f"{table}.csv"
    if csv_path.exists():
        return pd.read_csv(csv_path)

customers     = load_or_query("Customer")
accounts      = load_or_query("Accounts")
transactions  = load_or_query("Transactions")
transactions["day"] = pd.to_datetime(transactions["time"]).dt.date
sessions      = load_or_query("Sessions")
creditcard    = load_or_query("CreditCard")
loans         = load_or_query("Loans")
loanrequest   = load_or_query("LoanRequest")

customers["customer"] = customers["first_name"] + " " + customers["last_name"]

txn_by_type = transactions.groupby("t_type", as_index=False)\
                          .agg(txn_count=("tid","size"),
                               total_amount=("amount","sum"))

top_card = (creditcard.groupby("cust_id", as_index=False)
                       .agg(total_outstanding=("outstanding","sum"))
                       .merge(customers[["cust_id","customer"]])
                       .nlargest(5,"total_outstanding"))

multi_session = (sessions.groupby("cust_id", as_index=False)
                          .agg(session_count=("session_id","size"))
                          .query("session_count>1")
                          .merge(customers[["cust_id","customer"]]))

dormant = (accounts.merge(
              transactions[["source_acc_no","dest_acc_no"]]
                 .melt(var_name="role",value_name="account_no"),
              how="left",indicator=True)
           .query("_merge=='left_only'"))

last_txn = (transactions.merge(
                accounts[["account_no","cust_id"]]
                    .rename(columns={"account_no":"source_acc_no"}),
                how="left")
            .groupby("cust_id",as_index=False)
            .agg(last_time=("day","max"))
            .merge(customers[["cust_id","customer"]]))

loan_agg = loans.agg(total_principal=("principal","sum"),
                     total_paid=("paid","sum"))
loan_req_break = loanrequest.approval.value_counts(dropna=False).reset_index()
loan_req_break.columns = ["approval","count"]

fig_mix = px.bar(txn_by_type, x="t_type", y="txn_count", text="txn_count",
                 title="Transaction Mix by Type")
fig_mix.update_traces(textposition="outside")

fig_cards = px.bar(top_card.sort_values("total_outstanding"),
                   x="total_outstanding", y="customer", orientation="h",
                   title="Top 5 Credit-Card Outstanding")

fig_sess = px.bar(multi_session.sort_values("session_count"),
                  x="session_count", y="customer", orientation="h",
                  title="Customers with Multiple Sessions")

fig_dorm = px.scatter(dormant, x="account_no", y="balance",
                      title="Dormant Accounts with Balances",
                      labels={"account_no":"Account #","balance":"Balance ($)"})

fig_last = px.histogram(last_txn, x="last_time", nbins=30,
                        title="Distribution of Last Transaction Dates")

fig_loans = px.bar(loan_agg.melt(var_name="category",value_name="USD"),
                   x="category", y="USD", text="USD",
                   title="Loan Portfolio – Principal vs. Paid")

fig_req = px.pie(loan_req_break, values="count", names="approval",
                 title="Loan-Request Approval Funnel",
                 hole=0.4)

CAP = {
 "mix":  ("Deposits, withdrawals and transfers occur in balanced "
          "proportions. Deviations trigger anomaly alerts."),
 "cards":"Outstanding credit risk is concentrated in five customers; "
         "these accounts need stricter monitoring.",
 "sess": "Customers with many sessions provide stable behavioural "
         "profiles; abrupt changes may indicate credential theft.",
 "dorm": "Dormant, funded accounts are classic laundering targets.",
 "last": "Most customers transacted recently; long-inactive ones "
         "should be watched for sudden large withdrawals.",
 "loans":"Roughly {:.1%} of total principal has been repaid."
         .format(loan_agg.total_paid.iloc[0]/
                 loan_agg.total_principal.iloc[0]),
 "req":  "Approval funnel shows current conversion rate of loan requests."
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
    port = int(os.environ.get("PORT", 8050))
    app.run_server(host="0.0.0.0", port=port)
