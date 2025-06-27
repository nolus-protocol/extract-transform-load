# Synopsis

The Extract-Transform-Load component, ETL, aims to supply historical data for further data analysis. The data is obtained primarily from a software system referred to as the Blockchain System, and then loaded into a relational database referred to as the DB. The structure of the data does not undergo any changes. Some data attributes need value transformation in order to be easily consumed afterward.

# Architecture

The ETL component runs as a standalone service under the supervision of a system services manager, for example, systemctl. The service connects to the Blockchain System to receive events and query entities, a Market Data Provider to obtain aggregated market data, and the DB to push the data by inserting records. No updates, nor deletes of DB records.

# Global configuration

- an _endpoint_ to a Nolus node
- addresses of the _smart contract instances_ of interest, see this example as a reference for the structure

```
{
  "contracts_info": [
    {
      "treasury": {
        "instance": "nolus14hj2tavq8fpesdwxxcu44rty3hh90vhujrvcmstl4zr3txmfvw9s0k0puz"
      }
    },
    {
      "lpp": {
        "instance": "nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5"
      }
    },
    {
      "leaser": {
        "instance": "nolus1zwv6feuzhy6a9wekh96cd57lsarmqlwxdypdsplw6zhfncqw6ftqmx7chl"
      }
    },
    {
      "oracle": {
        "instance": "nolus1436kxs0w2es6xlqpp9rd35e3d0cjnw4sv8j3a7483sgks29jqwgsv3wzl4"
      }
    },
    {
      "profit": {
        "instance": "nolus1mf6ptkssddfmxvhdx0ech0k03ktp6kf9yk59renau2gvht3nq2gqkxgywu"
      }
    },
    {
      "rewards_dispatcher": {
        "instance": "nolus1wn625s4jcmvk0szpl85rj5azkfc6suyvf75q6vrddscjdphtve8s5gg42f"
      }
    }
  ]
}
```

- _market data provider_, type and endpoint, for example Coinmarketcap or Coingecko
- _DB connection parameters_
- _supported currencies_ - a set of (Nolus internal symbol, currency symbol, decimal digits) tuples providing the mapping between the currency symbols used internally by the system and their external equivalent. Currency symbols are unique.
- _stable currency_ - a symbol of the currency the market prices are into. The stable may or may not be amongst the supported currencies.
- _aggregation interval_ - number of hours between each aggregation. The interval is global so the records created at the end of an interval bear same timestamp.

# Data Model

The data model consists of _entities_ and aggregated _state_.

The _entity_ instances usually represent transactions, whereas the _state_ accumulates the effect of those transactions.

## Data Extraction Mechanism

### Entities

Except **MP_Asset** all other entities are received with Blockchain Events. The specification below defines the events and their attributes. Obtaining **MP_Asset** data is performed at regular intervals by a mechanism specific to the Market Data Provider API.

### State

Except **LP_Pool** all other state data is prepared by the System and is retrieved querying it at *aggregation interval*s. **LP_Pool** is populated initially as specified below.

### References

- [Events](https://docs.cosmos.network/master/core/events.html#)
- [Subscribing to Events](https://docs.tendermint.com/master/tendermint-core/subscription.html#)
- [Querying](https://docs.cosmwasm.com/docs/1.0/architecture/query#external-queries)

## Specification

The specification of all Data Types along with their attributes, types and source follows. By naming convention, a State Data Type ends in "_\_State_".

The precision of all date time values is up to a second.

### **MP_Asset** - Historical Data [Primary key = MP_asset_symbol + MP_asset_timestamp]

The price data is obtained from the market data provider once a preconfigured interval for all supported currencies. The prices are against the stable currency.

Config: time interval

| Property Name      | Type             | Description                                    |
| ------------------ | ---------------- | ---------------------------------------------- |
| MP_asset_symbol    | Alphanumeric(20) | Name of the asset                              |
| MP_asset_timestamp | Timestamp        | Date time at which the information is relevant |
| MP_price_in_stable | Decimal          | The price of the asset at this moment          |

# DEPRECATED
### **MP_Asset_State** - Historical Aggregated Data [Primary key = MP_asset_symbol + MP_timestamp]

| Property Name   | Type              | Query API                           | Description                                    |
| --------------- | ----------------- | ----------------------------------- | ---------------------------------------------- |
| MP_asset_symbol | Alphanumeric(20)  | Market Data Provider API            | Name of the asset                              |
| MP_timestamp    | Timestamp         | The end of <_aggregation interval_> | Date time at which the information is relevant |
| MP_price_open   | Decimal           | MP_Asset::MP_price_in_stable        | the open price of the asset in the interval    |
| MP_price_high   | Decimal           | MP_Asset::MP_price_in_stable        | the highest price of the asset in the interval |
| MP_price_low    | Decimal           | MP_Asset::MP_price_in_stable        | the lowest price of the asset in the interval  |
| MP_price_close  | Decimal           | MP_Asset::MP_price_in_stable        | the close price of the asset in the interval   |
| MP_volume       | Unsigned Int(128) | Market Data Provider API            | the asset volume amount in the interval        |
| MP_marketcap    | Unsigned Int(128) | Market Data Provider API            | the marketcap volume amount in the interval    |

### **LS_Opening** [Primary key = LS_contract_id]

| Property Name         | Type              | EventType.Attribute::Index                              | Description                                                                                              |
| --------------------- | ----------------- | ------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| LS_contract_id        | Alphanumeric(64)  | wasm-ls-open.id                                         | Lease Smart Contract ID                                                                                  |
| LS_address_id         | Alphanumeric(44)  | wasm-ls-open.customer                                   | Leaser's wallet address, ex.: nolus1qdhvz9d3an87vl0kwfj068szh74gfjjsq8rs44                               |
| LS_asset_symbol       | Alphanumeric(20)  | _supported currencies_[wasm-ls-open.currency]           | Asset currency symbol                                                                                    |
| LS_interest           | Unsigned Int      | wasm-ls-open.air                                        | Annual interest rate in permilles, ex.: 98 means 9.8%                                                    |
| LS_timestamp          | Timestamp         | wasm-ls-open.at                                         | Open time of the lease                                                                                   |
| LS_loan_pool_id       | Alphanumeric(64)  | wasm-ls-open.loan-pool-id                               | Liquidity Providers' Pool, LPP, Smart Contract ID                                                        |
| LS_loan_amnt_stable   | Unsigned Int(128) | in_stable(wasm-ls-open.loan-amount)                     | Loan amount in stable. The currency symbol is carried with wasm-ls-open.loan-symbol                      |
| LS_loan_amnt_asset    | Unsigned Int(128) | wasm-ls-open.loan-amount                                | Loan amount in asset currency. The currency symbol is carried with wasm-ls-open.loan-symbol              |
| LS_cltr_symbol        | Alphanumeric(20)  | _supported currencies_[wasm-ls-open.downpayment-symbol] | Collateral currency symbol                                                                               |
| LS_cltr_amnt_stable   | Unsigned Int(128) | in_stable(wasm-ls-open.downpayment-amount)              | Collateral amount in stable. The currency symbol is carried with wasm-ls-open.downpayment-symbol         |
| LS_cltr_amnt_asset    | Unsigned Int(128) | wasm-ls-open.downpayment-amount                         | Collateral amount in asset currency. The currency symbol is carried with wasm-ls-open.downpayment-symbol |
| LS_native_amnt_stable | Unsigned Int(128) | n/a                                                     | NLS amount in stable used to get discount [Rila version = 0]                                             |
| LS_native_amnt_nolus  | Unsigned Int(128) | n/a                                                     | NLS amount used to get discount [Rila version = 0]                                                       |
| LS_lpn_loan_amnt      | Unsigned Int(128) | n/a                                                     | lease amount in lpn                                                                                      |

### **LS_Closing** [Primary key = LS_contract_id] - claiming lease

| Property Name  | Type             | Event, Attribute | Description             |
| -------------- | ---------------- | ---------------- | ----------------------- |
| LS_contract_id | Alphanumeric(64) | wasm-ls-close.id | Lease Smart Contract ID |
| LS_timestamp   | Timestamp        | wasm-ls-close.at | Close time of the lease |
| Tx_Hash        | Alphanumeric(64) | tx hash          | Transaction hash        |

### **LS_Repayment** [Primary key = LS_repayment_height + LS_repayment_idx]

| Property Name              | Type              | Event, Attribute                                     | Description                                                     |
| -------------------------- | ----------------- | ---------------------------------------------------- | --------------------------------------------------------------- |
| LS_repayment_height        | Int(64)           | wasm-ls-repay.height                                 | Height of the block this transaction is in                      |
| LS_repayment_idx           | Int(32)           | wasm-ls-repay.idx                                    | Index in the block this transaction is at                       |
| LS_contract_id             | Alphanumeric(64)  | wasm-ls-repay.to                                     | Lease Smart Contract ID                                         |
| LS_payment_symbol                  | Alphanumeric(20)  | _supported currencies_[wasm-ls-repay.payment-symbol] | Repayment currency symbol                                       |
| LS_payment_amnt                    | Unsigned Int(128) | wasm-ls-repay.payment-amount                         | The amount of the transaction                                   |
| LS_payment_amnt_stable             | Unsigned Int(128) | in_stable(wasm-ls-repay.payment-amount)              | The amount of the transaction in stable                         |
| LS_timestamp               | Timestamp         | wasm-ls-repay.at                                     | Repay time                                                      |
| LS_loan_close              | Boolean           | wasm-ls-repay.loan-close                             | A flag indicating if this repayment closes the loan             |
| LS_prev_margin_stable      | Unsigned Int(128) | wasm-ls-repay.prev-margin-interest                   | The paid margin interest amount for the previous period, if any |
| LS_prev_interest_stable    | Unsigned Int(128) | wasm-ls-repay.prev-loan-interest                     | The paid loan interest amount for the previous period, if any   |
| LS_current_margin_stable   | Unsigned Int(128) | wasm-ls-repay.curr-margin-interest                   | The paid margin interest amount for the current period, if any  |
| LS_current_interest_stable | Unsigned Int(128) | wasm-ls-repay.curr-loan-interest                     | The paid loan interest amount for the current period, if any    |
| LS_principal_stable        | Unsigned Int(128) | wasm-ls-repay.principal                              | The paid principal, if any                                      |
| Tx_Hash                    | Alphanumeric(64)  | tx hash                                              | Transaction hash                                                |

### **LS_Liquidation** [Primary key = LS_liquidation_height + LS_liquidation_idx]

In case of full liquidation, in addition to the liquidation event, the system issues an event of closing that lease.

| Property Name              | Type              | Event, Attribute                                               | Description                                                                             |
| -------------------------- | ----------------- | -------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| LS_liquidation_height      | Int(64)           | wasm-ls-liquidation.height                                     | Height of the block this transaction is in                                              |
| LS_liquidation_idx         | Int(32)           | wasm-ls-liquidation.idx                                        | Index in the block this transaction is at                                               |
| LS_contract_id             | Alphanumeric(64)  | wasm-ls-liquidation.of                                         | Lease Smart Contract ID                                                                 |
| LS_amnt_symbol             | Alphanumeric(20)  | _supported currencies_[wasm-ls-liquidation.amount-symbol]      | Liquidation currency symbol                                                             |
| LS_amnt_stable             | Unsigned Int(128) | in_stable(wasm-ls-liquidation.amount-amount)                   | The amount of the transaction in stable                                                 |
| LS_liquidation_symbol      | Alphanumeric(20)  | wasm-ls-liquidation.liquidation-symbol                         | Liquidation currency symbol                                                             |
| LS_amnt                    | Unsigned Int(128) | wasm-ls-liquidation.amount-amount                              | The amount of the transaction                                                           |
| LS_payment_amnt            | Unsigned Int(128) | in_stable(wasm-ls-liquidation.payment-amount)                      | The amount of the transaction liquidated                                                |
| LS_payment_amnt_stable     | Unsigned Int(128) | in_stable(wasm-ls-liquidation.payment-amount)                      | The amount of the transaction in liquidation stable                                     |
| LS_timestamp               | Timestamp         | wasm-ls-liquidation.at                                         | Liquidation time                                                                        |
| LS_transaction_type        | Char(1)           | wasm-ls-liquidation.type                                       | 1 - Interest Overdue Liquidation, and 2 - Liability Exceeded Liquidation                |
| LS_prev_margin_stable      | Unsigned Int(128) | wasm-ls-liquidation.prev-margin-interest                       | The paid margin interest amount for the previous period, if 1 - always, if 2 - optional |
| LS_prev_interest_stable    | Unsigned Int(128) | wasm-ls-liquidation.prev-loan-interest                         | The paid loan interest amount for the previous period, if 1 - always, if 2 - optional   |
| LS_current_margin_stable   | Unsigned Int(128) | wasm-ls-liquidation.curr-margin-interest                       | The paid margin interest amount for the current period, if 1 - none, if 2 - optional    |
| LS_current_interest_stable | Unsigned Int(128) | wasm-ls-liquidation.curr-loan-interest                         | The paid loan interest amount for the current period, if 1 - none, if 2 - optional      |
| LS_principal_stable        | Unsigned Int(128) | wasm-ls-liquidation.principal                                  | The paid principal, if 1 - none, if 2 - optional                                        |
| Tx_Hash                    | Alphanumeric(64)  | tx hash                                                        | Transaction hash                                                                        |

### **LS_State** [Primary key = LS_contract_id + LS_timestamp] - include all unclaimed/not closed leases

ETL generates a record for any lease instance that:

- is present in the records of **LS_State** at the end of the previous period or in the **LS_Opening** records, and
- has not been closed, i.e. not present in **LS_Closing**

| Property Name              | Type              | Smart Contract, Query API                                | Description                                                                                             |
| -------------------------- | ----------------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| LS_contract_id             | Alphanumeric(64)  | n/a                                                      | Lease Smart Contract ID                                                                                 |
| LS_timestamp               | Timestamp         | The end of <_aggregation interval_>                      | Date time at which the information is relevant                                                          |
| LS_amnt_stable             | Unsigned Int(128) | in_stable(lease.status_query.amount::0)                  | The locked amount in the contract. Note: It may be less than the initial amount in case of liquidations |
| LS_amnt                    | Unsigned Int(128) | Token amount                                             | The locked amount in the contract. Note: It may be less than the initial amount in case of liquidations |
| LS_prev_margin_stable      | Unsigned Int(128) | in_stable(lease.status_query.margin_interest_overdue::0) | The margin interest amount for the previous period                                                      |
| LS_prev_interest_stable    | Unsigned Int(128) | in_stable(lease.status_query.loan_interest_overdue::0)   | The loan interest amount for the previous period                                                        |
| LS_current_margin_stable   | Unsigned Int(128) | in_stable(lease.status_query.margin_interest_due::0)     | The margin interest amount up to that point of time                                                     |
| LS_current_interest_stable | Unsigned Int(128) | in_stable(lease.status_query.loan_interest_due::0)       | The loan interest amount up to that point of time                                                       |
| LS_principal_stable        | Unsigned Int(128) | in_stable(lease.status_query.principal_due::0)           | The paid principal, if 1 - none, if 2 - optional                                                        |
| Tx_Hash                    | Alphanumeric(64)  | tx hash                                                  | Transaction hash                                                                                        |
| LS_lpn_loan_amnt           | Unsigned Int(128) | n/a                                                      | lease amount in lpn                                                                                     |

### **LP_Deposit** [Primary key = LP_deposit_height + LP_deposit_idx]

| Property Name     | Type              | Event, Attribute                          | Description                                                                                                                |
| ----------------- | ----------------- | ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| LP_deposit_height | Int(64)           | wasm-lp-deposit.height                    | Height of the block this transaction is in                                                                                 |
| LP_deposit_idx    | Int(32)           | wasm-lp-deposit.idx                       | Index in the block this transaction is at                                                                                  |
| LP_address_id     | Alphanumeric(44)  | wasm-lp-deposit.from                      | Liquidity Provider's wallet address, ex.: nolus1qdhvz9d3an87vl0kwfj068szh74gfjjsq8rs44                                     |
| LP_timestamp      | Timestamp         | wasm-lp-deposit.at                        | Deposit time                                                                                                               |
| LP_Pool_id        | Alphanumeric(44)  | wasm-lp-deposit.to                        | Liquidity Providers' Pool, LPP, Smart Contract ID                                                                          |
| LP_amnt_stable    | Unsigned Int(128) | in_stable(wasm-lp-deposit.deposit-amount) | Deposited amount in stable. The currency symbol is carried with wasm-lp-deposit.deposit-symbol                             |
| LP_amnt_asset     | Unsigned Int(128) | wasm-lp-deposit.deposit-amount            | Deposited amount in asset currency = LP_Pool.LP_symbol. The currency symbol is carried with wasm-lp-deposit.deposit-symbol |
| LP_amnt_receipts  | Unsigned Int(128) | wasm-lp-deposit.receipts                  | Number of receipts issued, nLPN                                                                                            |
| LS_lpn_loan_amnt  | Unsigned Int(128) | n/a                                       | lease amount in lpn                                                                                                        |

### **LP_Withdraw** [Primary key = LP_withdraw_height + LP_withdraw_idx]

| Property Name      | Type              | Event, Attribute                            | Description                                                                                                                  |
| ------------------ | ----------------- | ------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| LP_withdraw_height | Int(64)           | wasm-lp-withdraw.height                     | Height of the block this transaction is in                                                                                   |
| LP_withdraw_idx    | Int(32)           | wasm-lp-withdraw.idx                        | Index in the block this transaction is at                                                                                    |
| LP_address_id      | Alphanumeric(44)  | wasm-lp-withdraw.to                         | Liquidity Provider's wallet address, ex.: nolus1qdhvz9d3an87vl0kwfj068szh74gfjjsq8rs44                                       |
| LP_timestamp       | Timestamp         | wasm-lp-withdraw.at                         | Withdraw time                                                                                                                |
| LP_Pool_id         | Alphanumeric(64)  | wasm-lp-withdraw.from                       | Liquidity Providers' Pool, LPP, Smart Contract ID                                                                            |
| LP_amnt_stable     | Unsigned Int(128) | in_stable(wasm-lp-withdraw.withdraw-amount) | Withdrawn amount in stable. The currency symbol is carried with wasm-lp-withdraw.withdraw-symbol                             |
| LP_amnt_asset      | Unsigned Int(128) | wasm-lp-withdraw.withdraw-amount            | Withdrawn amount in asset currency = LP_Pool.LP_symbol. The currency symbol is carried with wasm-lp-withdraw.withdraw-symbol |
| LP_amnt_receipts   | Unsigned Int(128) | wasm-lp-withdraw.receipts                   | Number of receipts burned, nLPN                                                                                              |
| LP_deposit_close   | Boolean           | wasm-lp-withdraw.close                      | A flag indicating if this withdraw closes the deposited amounts                                                              |
| Tx_Hash            | Alphanumeric(64)  | tx hash                                     | Transaction hash                                                                                                             |

### **LP_Lender_State** [Primary key = LP_Lender_id + LP_Pool_id + LP_timestamp]

ETL generates a record for any lender instance that:

- is present in the records of **LP_Lender_State** at the end of the previous period or in the **LP_Deposit** records, and
- has not been closed, i.e. not present in **LP_Withdraw** OR **LP_Withdraw.LP_deposit_close** = False

| Property Name      | Type              | Smart Contract, Query API                              | Description                                                                            |
| ------------------ | ----------------- | ------------------------------------------------------ | -------------------------------------------------------------------------------------- |
| LP_Lender_id       | Alphanumeric(44)  | n/a                                                    | Liquidity Provider's wallet address, ex.: nolus1qdhvz9d3an87vl0kwfj068szh74gfjjsq8rs44 |
| LP_Pool_id         | Alphanumeric(64)  | n/a                                                    | Liquidity Providers' Pool, LPP, Smart Contract ID                                      |
| LP_timestamp       | Timestamp         | The end of <_aggregation interval_>                    | Date time at which the information is relevant                                         |
| LP_Lender_stable   | Unsigned Int(128) | in_stable(int(lpp.balance.balance \* lpp.price.price)) | The lender's total amount of pool's native asset in stable                             |
| LP_Lender_asset    | Unsigned Int(128) | int(lpp.balance.balance \* lpp.price.price)            | The lender's total amount of pool's native asset                                       |
| LP_Lender_receipts | Unsigned Int(128) | lpp.balance.balance                                    | The lender's total amount of pool's receipts                                           |

### **LP_Pool** [Primary Key = LP_Pool_id]

Populated initially with an instance per an Lpp smart contract instance configured with _smart contract instances_. Consult the Lpp query API on details of how to obtain Lpp currency symbol. It is one of the _supported currencies_ internal currency symbols.

| Property Name | Type             | Origin                                              | Description                                                       |
| ------------- | ---------------- | --------------------------------------------------- | ----------------------------------------------------------------- |
| LP_Pool_id    | Alphanumeric(64) | Smart Contract ID                                   | Liquidity Providers' Pool, LPP, Smart Contract Instance ID        |
| LP_symbol     | Alphanumeric(20) | _supported currencies_[lpp.query_config.lpn_symbol] | Deposit currency symbol, Liquidity Provider Native Currency (LPN) |

### **LP_Pool_State** [Primary Key = LP_Pool_id + LP_Pool_timestamp]

ETL generates a record for any lender instance present in **LP_Pool**

| Property Name                     | Type              | Smart Contract, Query API                                                                                     | Description                                                                     |
| --------------------------------- | ----------------- | ------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| LP_Pool_id                        | Alphanumeric(64)  | Smart Contract ID                                                                                             | Liquidity Providers' Pool, LPP, Smart Contract Instance ID                      |
| LP_Pool_timestamp                 | Timestamp         | The end of <_aggregation interval_>                                                                           | Date time at which the information is relevant                                  |
| LP_Pool_total_value_locked_stable | Unsigned Int(128) | in_stable(lpp.lpp_balance.balance + lpp.lpp_balance.total_principal_due + lpp.lpp_balance.total_interest_due) | The total value locked amount of pool's native asset in stable                  |
| LP_Pool_total_value_locked_asset  | Unsigned Int(128) | lpp.lpp_balance.balance + lpp.lpp_balance.total_principal_due + lpp.lpp_balance.total_interest_due            | The total value locked amount of pool's native asset in LP_Pool.LP_symbol = LPN |
| LP_Pool_total_issued_receipts     | Unsigned Int(128) | lpp.lpp_balance.balance_nlpn                                                                                  | The total amount of pool's receipts issued to the lenders on each deposit       |
| LP_Pool_total_borrowed_stable     | Unsigned Int(128) | in_stable(lpp.lpp_balance.total_principal_due)                                                                | The total amount borrowed from pool's native asset in stable                    |
| LP_Pool_total_borrowed_asset      | Unsigned Int(128) | lpp.lpp_balance.total_principal_due                                                                           | The total amount borrowed from pool's native asset in LP_Pool.LP_symbol = LPN   |
| LP_Pool_total_yield_stable        | Unsigned Int(128) | 0                                                                                                             | The total amount in yield in stable                                             |
| LP_Pool_total_yield_asset         | Unsigned Int(128) | 0                                                                                                             | The total amount in yield in LP_Pool.LP_symbol = LPN                            |

### **MP_Yield - Historical Data** [Primary key = MP_yield_symbol + MP_yield_timestamp]

| Property Name      | Type             | Query API | Description                                    |
| ------------------ | ---------------- | --------- | ---------------------------------------------- |
| MP_yield_symbol    | Alphanumeric(20) | TBD       | Unique name of the yield source                |
| MP_yield_timestamp | Timestamp        | TBD       | Date time at which the information is relevant |
| MP_apy_permilles   | Int(16)          | TBD       | Annual Percentage Yield                        |

### **TR_Profit - Transfers from bought-back NLS into Treasury** [Primary key = TR_Profit_height + TR_Profit_idx]

| Property Name         | Type              | Event, Attribute                        | Description                                                                                        |
| --------------------- | ----------------- | --------------------------------------- | -------------------------------------------------------------------------------------------------- |
| TR_Profit_height      | Int(64)           | wasm-tr-profit.height                   | Height of the block this transaction is in                                                         |
| TR_Profit_idx         | Int(32)           | wasm-tr-profit.idx                      | Index in the block this transaction is at                                                          |
| TR_Profit_timestamp   | Timestamp         | wasm-tr-profit.at                       | Date time of the profit transfer                                                                   |
| TR_Profit_amnt_stable | Unsigned Int(128) | in_stable(wasm-tr-profit.profit-amount) | The amount transferred in stable. The currency symbol is carried with wasm-tr-profit.profit-symbol |
| TR_Profit_amnt_nls    | Unsigned Int(128) | wasm-tr-profit.profit-amount            | The amount transferred in NLS. The currency symbol is carried with wasm-tr-profit.profit-symbol    |
| Tx_Hash               | Alphanumeric(64)  | tx hash                                 | Transaction hash                                                                                   |

### **TR_Rewards_Distribution - Transfers from Treasury to the Liquidity Pools** [Primary key = TR_Rewards_height + TR_Rewards_idx + TR_Rewards_Pool_id]

| Property Name          | Type              | Event, Attribute                          | Description                                                                                          |
| ---------------------- | ----------------- | ----------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| TR_Rewards_height      | Int(64)           | wasm-tr-rewards.height                    | Height of the block this transaction is in                                                           |
| TR_Rewards_idx         | Int(32)           | wasm-tr-rewards.idx                       | Index in the block this transaction is at                                                            |
| TR_Rewards_Pool_id     | Alphanumeric(64)  | wasm-tr-rewards.to                        | Liquidity Providers' Pool, LPP, Smart Contract ID                                                    |
| TR_Rewards_timestamp   | Timestamp         | wasm-tr-rewards.at                        | Date time of the reward distribution                                                                 |
| TR_Rewards_amnt_stable | Unsigned Int(128) | in_stable(wasm-tr-rewards.rewards-amount) | The amount transferred in stable. The currency symbol is carried with wasm-tr-rewards.rewards-symbol |
| TR_Rewards_amnt_nls    | Unsigned Int(128) | wasm-tr-rewards.rewards-amount            | The amount transferred in NLS. The currency symbol is carried with wasm-tr-rewards.rewards-symbol    |
| Tx_Hash                | Alphanumeric(64)  | tx hash                                   | Transaction hash                                                                                     |

### **TR_State** [Primary Key = TR_timestamp]

The balance of the Treasury is obtained by querying the system with a Bank Balances message. The result is a list of coins. ETL must report an error if there is more than one coin.

| Property Name  | Type              | Smart Contract, Query API           | Description                                    |
| -------------- | ----------------- | ----------------------------------- | ---------------------------------------------- |
| TR_timestamp   | Timestamp         | The end of <_aggregation interval_> | Date time at which the information is relevant |
| TR_amnt_stable | Unsigned Int(128) | in_stable(bank.balances[0]::0)      | The total amount in yield in stable            |
| TR_amnt_nls    | Unsigned Int(128) | bank.balances[0]::0                 | The total amount in yield in NLS               |

### **PL_State** [Primary key = PL_timestamp]

Aggragation is done over all records pertaining to the same _aggregation interval_. For example, PL_pools_TVL_stable is equal to the sum of LP_Pool_State::LP_Pool_total_value_locked_stable for all LP_Pool_id up to the same interval.

| Property Name                        | Type              | Smart Contract, Query API                                                                                                                                                                        | Description                                                                                                                                                                                                 |
| ------------------------------------ | ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PL_timestamp                         | Timestamp         | The end of <_aggregation interval_>                                                                                                                                                              | Date time at which the information is relevant                                                                                                                                                              |
| PL_pools_TVL_stable                  | Unsigned Int(140) | sum(LP_Pool_State::LP_Pool_total_value_locked_stable) for all records with LP_Pool_timestamp == PL_timestamp                                                                                     | Total locked Funds in all pool instances                                                                                                                                                                    |
| PL_pools_borrowed_stable             | Unsigned Int(140) | sum(LP_Pool_State::LP_Pool_total_borrowed_stable) for all records with LP_Pool_timestamp == PL_timestamp                                                                                         | Total borrowed Funds in all pool instances                                                                                                                                                                  |
| PL_pools_yield_stable                | Unsigned Int(140) | sum(LP_Pool_State::LP_Pool_total_yield_stable) for all records with LP_Pool_timestamp == PL_timestamp                                                                                            | Total Funds in yield from all pool instances                                                                                                                                                                |
| PL_LS_count_open                     | Unsigned Int(64)  | count(LS_State) all records with LS_Timestamp == PL_timestamp                                                                                                                                    | Total Lease Accounts in open state, i.e. with pending loans or unclaimed                                                                                                                                    |
| PL_LS_count_closed                   | Unsigned Int(64)  | count(LS_Closing) all records with LS_Timestamp within the interval                                                                                                                              | Total Lease Accounts closed since the last Platform snapshot                                                                                                                                                |
| PL_LS_count_opened                   | Unsigned Int(64)  | count(LS_Opening) all records with LS_Timestamp within the interval                                                                                                                              | Total Lease Accounts opened since the last Platform snapshot                                                                                                                                                |
| PL_IN_LS_cltr_amnt_opened_stable     | Unsigned Int(140) | sum(LS_Opening::LS_cltr_amnt_stable) all records with LS_Timestamp within the interval                                                                                                           | Total collateral received on opening of Lease Accounts since the last Platform snapshot                                                                                                                     |
| PL_LP_count_open                     | Unsigned Int(64)  | count(LP_Lender_State) all records with LP_Timestamp == PL_timestamp                                                                                                                             | Total Deposit Accounts in open state, i.e. with a positive balance                                                                                                                                          |
| PL_LP_count_closed                   | Unsigned Int(64)  | count(LP_Withdraw) all records with LP_Timestamp within the interval and LP_deposit_close == True                                                                                                | Total Deposit Accounts closed since the last Platform snapshot, i.e. withdrawn the whole amount                                                                                                             |
| PL_LP_count_opened                   | Unsigned Int(64)  | count(LP_Deposit) all records with LP_Timestamp within the interval                                                                                                                              | Total Deposit Accounts opened since the last Platform snapshot, i.e. new deposits by new lenders                                                                                                            |
| PL_OUT_LS_loan_amnt_stable           | Unsigned Int(140) | sum(LS_Opening::LS_loan_amnt_stable) all records with LS_Timestamp within the interval                                                                                                           | Total amount of issued loans in stable since the last Platform snapshot                                                                                                                                     |
| PL_IN_LS_rep_amnt_stable             | Unsigned Int(140) | sum(next four PL*IN_LS_rep*\*)                                                                                                                                                                   | Total amount of repayments in stable since the last Platform snapshot                                                                                                                                       |
| PL_IN_LS_rep_prev_margin_stable      | Unsigned Int(140) | sum(LS_Repayment::LS_prev_margin_stable) all records with LS_Timestamp within the interval                                                                                                       | Total amount of margin amounts for the previous period since the last Platform snapshot                                                                                                                     |
| PL_IN_LS_rep_prev_interest_stable    | Unsigned Int(140) | sum(LS_Repayment::LS_prev_interest_stable) all records with LS_Timestamp within the interval                                                                                                     | Total amount of interest amounts for the previous period since the last Platform snapshot                                                                                                                   |
| PL_IN_LS_rep_current_margin_stable   | Unsigned Int(140) | sum(LS_Repayment::LS_current_margin_stable) all records with LS_Timestamp within the interval                                                                                                    | Total amount of margin amounts for the current period since the last Platform snapshot                                                                                                                      |
| PL_IN_LS_rep_current_interest_stable | Unsigned Int(140) | sum(LS_Repayment::LS_current_interest_stable) all records with LS_Timestamp within the interval                                                                                                  | Total amount of interest amounts for the current period since the last Platform snapshot                                                                                                                    |
| PL_IN_LS_rep_principal_stable        | Unsigned Int(140) | sum(LS_Repayment::LS_principal_stable) all records with LS_Timestamp within the interval                                                                                                         | Total amount of paid principal amounts since the last Platform snapshot                                                                                                                                     |
| PL_OUT_LS_cltr_amnt_stable           | Unsigned Int(140) | sum(LS_Opening::LS_cltr_amnt_stable) for all records LS_Opening::LS_contract_id == LS_Closing::LS_contract_id and LS_Closing::LS_Timestamp within the interval                                   | Total amount of released collateral amount in stable since the last Platform snapshot                                                                                                                       |
| PL_OUT_LS_amnt_stable                | Unsigned Int(140) | sum(LS_Opening::LS_loan_amnt_stable + LS_Opening::LS_cltr_amnt_stable) for all records LS_Opening::LS_contract_id == LS_Closing::LS_contract_id and LS_Closing::LS_Timestamp within the interval | Total amount of released lease amounts in stable since the last Platform snapshot                                                                                                                           |
| PL_native_amnt_stable                | Unsigned Int(140) | 0                                                                                                                                                                                                | Total NLS amount in stable used to get discount since the last Platform snapshot                                                                                                                            |
| PL_native_amnt_nolus                 | Unsigned Int(140) | 0                                                                                                                                                                                                | Total NLS amount used to get discount since the last Platform snapshot                                                                                                                                      |
| PL_IN_LP_amnt_stable                 | Unsigned Int(140) | sum(LP_Deposit::LP_amnt_stable) for all records with LP_Timestamp within the interval                                                                                                            | Total deposited amount in stable since the last Platform snapshot                                                                                                                                           |
| PL_OUT_LP_amnt_stable                | Unsigned Int(140) | sum(LP_Withdraw::LP_amnt_stable) for all records with LP_Timestamp within the interval                                                                                                           | Total withdrawn amount in stable since the last Platform snapshot                                                                                                                                           |
| PL_TR_profit_amnt_stable             | Unsigned Int(140) | sum(TR_Profit::TR_Profit_amnt_stable) for all records with TR_Profit_timestamp within the interval                                                                                               | Total amount bought-back in stable since the last Platform snapshot                                                                                                                                         |
| PL_TR_profit_amnt_nls                | Unsigned Int(140) | sum(TR_Profit::TR_Profit_amnt_nls) for all records with TR_Profit_timestamp within the interval                                                                                                  | Total amount bought-back in NLS since the last Platform snapshot                                                                                                                                            |
| PL_TR_tax_amnt_stable                | Unsigned Int(128) | TR_State::TR_amnt_stable + PL_OUT_TR_rewards_amnt_stable - PL_TR_profit_amnt_stable - TR_State::TR_amnt_stable at the end of the previous period                                                 | Total amount of the additional tax charged on each transaction in stable since the last Platform snapshot, state_old + tax + profit - rewards = state_now => tax = state_now + rewards - profit - state_old |
| PL_TR_tax_amnt_nls                   | Unsigned Int(128) | TR_State::TR_amnt_nls + PL_OUT_TR_rewards_amnt_nls - PL_TR_profit_amnt_nls - TR_State::TR_amnt_nls at the end of the previous period                                                             | Total amount of the additional tax charged on each transaction in NLS since the last Platform snapshot                                                                                                      |
| PL_OUT_TR_rewards_amnt_stable        | Unsigned Int(140) | sum(TR_Rewards_Distribution::TR_Rewards_amnt_stable) for all records with TR_Rewards_Distribution::TR_Rewards_timestamp within the interval                                                      | Total amount transferred in stable since the last Platform snapshot                                                                                                                                         |
| PL_OUT_TR_rewards_amnt_nls           | Unsigned Int(140) | sum(TR_Rewards_Distribution::TR_Rewards_amnt_nls) for all records with TR_Rewards_Distribution::TR_Rewards_timestamp within the interval                                                         | Total amount transferred in NLS since the last Platform snapshot                                                                                                                                            |

### **LS_Close_Position** [Primary Key = LS_position_height + LS_position_idx]

| Property Name              | Type              | EventType.Attribute::Index                           | Description                                                     |
| -------------------------- | ----------------- | ---------------------------------------------------- | --------------------------------------------------------------- |
| LS_position_height         | Int(64)           | wasm-ls-close.height                                 | Height of the block this transaction is in                      |
| LS_position_idx            | Int(32)           | wasm-ls-close.idx                                    | Index in the block this transaction is at                       |
| LS_contract_id             | Alphanumeric(64)  | wasm-ls-close.to                                     | Lease Smart Contract ID                                         |
| LS_change                  | Unsigned Int(128) | wasm-ls-close.change                                 | change after market close position                              |
| LS_amnt                    | Unsigned Int(128) | wasm-ls-close.amount_amount                          |
| LS_amnt_symbol             | Alphanumeric(20)  | wasm-ls-close.LS_amount_symbol                       |
| LS_amnt_stable             | Unsigned Int(128) | in_stable(wasm-ls-close.amount_amount)               | The amount of the transaction in stable                         |
| LS_payment_amnt            | Unsigned Int(128) | wasm-ls-close.payment_amount                         |
| LS_payment_symbol          | Alphanumeric(20)  | wasm-ls-close.LS_payment_symbol                      |
| LS_payment_amnt_stable     | Unsigned Int(128) | in_stable(wasm-ls-close.payment_amount)              | The amount of the payment transaction in stable                 |
| LS_timestamp               | Timestamp         | wasm-ls-close.at                                     | Open time of the lease                                          |
| LS_loan_close              | Boolean           | wasm-ls-close.loan-close                             | A flag indicating if this repayment closes the loan             |
| LS_prev_margin_stable      | Unsigned Int(128) | wasm-ls-close.prev-margin-interest                   | The paid margin interest amount for the previous period, if any |
| LS_prev_interest_stable    | Unsigned Int(128) | wasm-ls-close.prev-loan-interest                     | The paid loan interest amount for the previous period, if any   |
| LS_current_margin_stable   | Unsigned Int(128) | wasm-ls-close.curr-margin-interest                   | The paid margin interest amount for the current period, if any  |
| LS_current_interest_stable | Unsigned Int(128) | wasm-ls-close.curr-loan-interest                     | The paid loan interest amount for the current period, if any    |
| LS_principal_stable        | Unsigned Int(128) | wasm-ls-close.principal                              | The paid principal, if any                                      |
| Tx_Hash                    | Alphanumeric(64)  | tx hash                                              | Transaction hash                                                |

### **LS_Liquidation_Warning** - Historical Aggregated Data [Primary key = Tx_Hash + LS_contract_id + LS_timestamp]

| Property Name   | Type              | Query API                               | Description                                    |
| --------------- | ----------------- | ----------------------------------------| ---------------------------------------------- |
| LS_contract_id  | Alphanumeric(64)  | wasm-ls-liquidation-warning.lease       | Lease address                                  |
| LS_address_id   | Alphanumeric(44)  | wasm-ls-liquidation-warning.customer    | User address                                   |
| LS_asset_symbol | Alphanumeric(20)  | wasm-ls-liquidation-warning.lease-asset | Lease currency symbol                          |
| LS_level        | SMALLINT          | wasm-ls-liquidation-warning.level       | Lease level                                    |
| LS_ltv          | SMALLINT          | wasm-ls-liquidation-warning.ltv         | Lease ltv                                      |
| LS_timestamp    | Timestamp         | timestamp | Block timestsamp            | Timestamp                                      |
| Tx_Hash         | Alphanumeric(64)  | tx hash                                 | Transaction hash                               |

### **Reserve_Cover_Loss** - Historical Aggregated Data [Primary key = LS_contract_id + Event_Block_Index + Tx_Hash]

| Property Name     | Type              | Query API                               | Description                                    |
| ----------------- | ----------------- | ----------------------------------------| ---------------------------------------------- |
| LS_contract_id    | Alphanumeric(64)  | wasm-reserve-cover-loss.to              | Lease address                                  |
| Event_Block_Index | INT               |                                         | index in trasaction                            |
| Tx_Hash           | Alphanumeric(64)  |                                         | Transaction hash                               |
| LS_symbol         | Alphanumeric(20)  | wasm-reserve-cover-loss.payment-amount  | The amount symbol in transaction               |
| LS_amnt           | Unsigned Int(128) | wasm-reserve-cover-loss.payment-symbol  | The amount in transaction                      |

### **LS_Loan_Closing** - Historical Aggregated Data [Primary key = LS_contract_id + Event_Block_Index + Tx_Hash]

| Property Name     | Type              | Query API                               | Description                                    |
| ----------------- | ----------------- | ----------------------------------------| ---------------------------------------------- |
| LS_contract_id    | Alphanumeric(64)  |                                         | Lease address                                  |
| LS_symbol         | Alphanumeric(20)  |                                         | The amount symbol in transaction               |
| LS_amnt           | Unsigned Int(128) |                                         | The amount in transaction                      |
| LS_amnt_stable    | Unsigned Int(128) |                                         | The amount in transaction in stable            |
| LS_timestamp      | Timestamp         | timestamp | Block timestsamp            | Timestamp                                      |
| Type              | Alphanumeric(64)  |                                         | Closing event type                             |
| Block             | Int(64)           |                                         | Block when loan is closed                      |
| Active            | Boolean           |                                         | Indicates if loan is synced with blochain      |


### **LS_Slippage_Anomaly** - Historical Aggregated Data [Primary key = Tx_Hash + LS_contract_id + LS_timestamp]

| Property Name   | Type              | Query API                               | Description                                    |
| --------------- | ----------------- | ----------------------------------------| ---------------------------------------------- |
| LS_contract_id  | Alphanumeric(64)  | wasm-ls-slippage-anomaly.lease          | Lease address                                  |
| LS_address_id   | Alphanumeric(44)  | wasm-ls-slippage-anomaly.customer       | User address                                   |
| LS_asset_symbol | Alphanumeric(20)  | wasm-ls-slippage-anomaly.lease-asset    | Lease currency symbol.                         |
| LS_max_slipagge | SMALLINT          | wasm-ls-slippage-anomaly.level          | Lease max slippage.                            |
| LS_timestamp    | Timestamp         | timestamp | Block timestsamp            | Timestamp                                      |
| Tx_Hash         | Alphanumeric(64)  | tx hash                                 | Transaction hash                               |


#### Database types

Due to the lack of unsigned integer types in the databases with more that 64 bits we define the mapping of Unsigned Int(128) as Decimal(log(2 ** 128, 10), 0), i.e. Decimal(39, 0). Simmilarly, the fields that accumulate such values we map into Decimal(log(2 ** 140, 10), 0), i.e. Decimal(42, 0).

# Logging

Any significant event that may further be used for troubleshooting or user information should be logged out from the system. The destination of the logs is the system log.
