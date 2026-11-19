# Regional E-Commerce Analytics: Business Overview

This document explains what the system does for the business and which rules it applies to the
data. It avoids technical detail; for how it is built, see `README.md`.

---

## 1. What problem does it solve?

An online retailer selling to customers in four US regions (Northeast, South, Midwest and West)
receives a constant stream of order records. Those records are messy. The same order can arrive
twice, dates are written in several different styles, prices and quantities are sometimes negative
or missing, and some orders point to customers or products that do not exist.

If that data went straight into reports, the numbers would be wrong, and nobody would know by how
much. This system:

1. **Takes in** the raw order feed every few minutes.
2. **Cleans it**, using explicit and documented rules, and sets aside anything it cannot trust.
3. **Produces trusted reports**: revenue, best customers, product performance and customer segments.
4. **Proves the numbers are right** by checking the two places the data is stored against each other.

The result is a database that analysts and dashboards can query with confidence, plus a clear
record of every order that was rejected and why.

---

## 2. What data comes in?

| Feed | What it contains | Example |
|---|---|---|
| **Orders** | One line per order: customer, product, quantity, unit price, discount, order date | "Customer 465 bought 10 monitors at 329.50 with a 14.54 discount on 8 Nov 2025" |
| **Customers** | The customer list: name, phone, city, state, region | "Hannah Jones, Philadelphia, PA, Northeast" |
| **Products** | The product catalogue: name, category, list price | "Wireless Ergonomic Mouse, Electronics, 29.99" |

Customers and products are the **master lists**. They are the single source of truth for names,
cities, regions and categories. Orders only need to point at them by ID, so a blank customer name or
city on an order line does not matter.

New orders are produced every 2 minutes. This project uses a synthetic feed that is deliberately
messy, to show how the system copes with real-world problems.

---

## 3. How the numbers are calculated

| Term | Meaning |
|---|---|
| **Gross revenue** | Quantity × unit price |
| **Discount** | The amount taken off the order. A blank discount means *no discount* (zero). |
| **Net revenue** | Gross revenue − discount. This is the figure used in every report. |
| **Average order value** | Net revenue ÷ number of orders |

All money is held to the cent (two decimal places) without rounding drift, so totals add up exactly.

---

## 4. Which orders are accepted, and which are rejected?

Every order is checked against these rules, in this order. **The first rule an order fails is the
reason recorded.** Rejected orders are not deleted: they are kept in a separate **quarantine** area,
with the reason, so they can be reviewed.

| # | Rule | Reason shown |
|---|---|---|
| 1 | The order has an order ID | Missing order ID |
| 2 | The order date can be read | Invalid date |
| 3 | Quantity is a positive number | Invalid quantity |
| 4 | Unit price is not negative or blank | Invalid price |
| 5 | Discount is not negative | Negative discount |
| 6 | The discount does not exceed the order value (net revenue is not negative) | Discount exceeds gross |
| 7 | The customer exists in the customer list | Unknown customer |
| 8 | The product exists in the product catalogue | Unknown product |

**Duplicates:** if the same order ID arrives more than once, only one copy is kept.

**Dates:** orders arrive with dates written in five different styles (for example `2025-11-08`,
`11/08/2025`, `08-11-2025`). Some styles can be read two ways (day first or month first), so the
system decides by the pattern of the text. If you rely on dates from a specific source, confirm
which style it uses.

**Late customers and products:** an order for a customer or product that is not on the list *yet*
is not lost forever. Orders rejected as "unknown customer" or "unknown product" are re-checked on
every run, and move into the reports automatically once the customer or product shows up. All other
rejections are final, because the order itself is faulty.

**Quality report:** after each batch the system records how many orders passed, how many were
rejected, and the breakdown by reason. In the sample feed, roughly one order in eight is rejected,
mostly for bad prices or quantities.

---

## 5. What reports does the business get?

Five ready-made reports are produced from the accepted orders.

### Monthly revenue
Per month: number of orders, units sold, gross revenue, total discounts, net revenue and average
order value. **Answers:** *Are we growing? How much do we give away in discounts?*

### Top customers per region
Customers ranked by total spend within each region, with orders, spend and average order value. If
two customers spent exactly the same, ties are broken in a fixed way so the ranking never changes
between runs. **Answers:** *Who are our best customers in each region?*

### Category share
For each year and product category: units sold, net revenue and the category's **share of that
year's revenue**. **Answers:** *Which categories drive the business, and is the mix shifting?*

### Product performance
For each product: orders, units, revenue, discounts and **discount rate** (discounts as a
percentage of gross revenue), plus its rank within its category and overall. **Answers:** *What
sells best? Which products depend on heavy discounting?*

### Customer segments (RFM)
Each customer is scored from 1 to 4 on three questions, then placed in a segment:

- **Recency:** how recently did they order? (within 30 days = 4, 90 = 3, 180 = 2, older = 1)
- **Frequency:** how many orders? (10 or more = 4, 5+ = 3, 2+ = 2, one order = 1)
- **Monetary:** how much have they spent? (1,000 or more = 4, 500+ = 3, 200+ = 2, under 200 = 1)

| Segment | Who they are |
|---|---|
| **Champions** | Recent, frequent and high-spending (all three scores 3 or 4) |
| **Loyal Customers** | Order often and spend a fair amount |
| **At Risk** | Used to order often, but not recently: worth a win-back offer |
| **New Customers** | Ordered recently, but only once so far |
| **Hibernating / Lost** | Not seen for a long time and rarely ordered |
| **Potential Loyalists** | Everyone in between: room to grow |

**Answers:** *Who should we reward, who should we win back, and who is new?* "Recency" is measured
against the date of the most recent order in the data, not today's date.

---

## 6. How do we know the numbers are right?

After every refresh, an automatic **reconciliation** step compares the two copies of the data (the
working data store and the reporting database) on:

- the number of customers, products, dates and orders
- total orders, units, gross revenue, discounts and net revenue
- revenue for every month
- revenue for every product category
- the top spender in every region

If **any** figure differs, the run is flagged as failed and a report of the differences is saved.
Bad numbers are stopped before anyone uses them.

---

## 7. How often is it updated?

| What | How often |
|---|---|
| New orders arrive | Every 2 minutes |
| Reports are refreshed | Every 30 minutes (a refresh takes roughly 10 to 15 minutes) |

Reports therefore reflect orders from within about the last hour. Each refresh only processes
orders it has not seen before, so a run does not redo old work. If a refresh is interrupted, running
it again is safe: nothing is counted twice and nothing is lost.

---

## 8. An order's journey

> Order `f98c29b3…`: Customer 465 buys 10 "4K Ultra HD Monitor" at a unit price of **-329.50**.

1. **Arrives** in the order feed and is stored exactly as received.
2. **Checked:** the order ID exists, the date is readable, the quantity is positive, but the unit
   price is negative, so it fails rule 4.
3. **Set aside** in quarantine with the reason "Invalid price". It is excluded from every report.
4. It appears in the quality report's count of rejected orders, so the data team can see the
   problem and raise it with whoever supplies the feed.

A valid order follows the same path, passes every rule, has its revenue calculated, and is
included in the monthly revenue, category share, product performance, customer ranking and
customer segment reports.

---

## 9. Limits to be aware of

- **The data is synthetic.** Customers, products and orders are generated to look realistic, so
  none of the figures describe a real business.
- **One currency.** Amounts are treated as a single currency; there is no currency conversion.
- **Master lists are small and stable.** The customer and product lists are refreshed on every run,
  but are assumed to change rarely.
- **Rejected orders are not corrected automatically.** Only orders waiting on a missing customer or
  product are retried. Orders with faulty prices, quantities or dates need to be fixed at the source.

---

## Glossary

- **Quarantine:** the holding area for rejected orders, kept with the reason for rejection.
- **Master data:** the customer list and product catalogue.
- **Net revenue:** order value after discount; the figure used in reports.
- **RFM:** Recency, Frequency, Monetary: a standard way to score customers.
- **Reconciliation:** automatically checking that two copies of the data agree.
