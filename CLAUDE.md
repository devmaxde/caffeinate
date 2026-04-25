# Caffeinate - Qontext Layer Evaluation Suite

## What this is

We're building a **qontext layer** for organization data — a system that aggregates noisy enterprise data (emails, chats, code, HR records, sales, support tickets) and answers business questions by surgically extracting relevant context.

**EnterpriseBench** (from Inazuma.co, a D2C enterprise) is our source dataset. It contains realistic enterprise data across 14+ domains: employees, emails, conversations, GitHub repos, CRM, sales, products, customer support, IT tickets, social posts, vendors, clients, policy docs, and invoices.

## Evaluation approach

Evals are **(I, Q, C) triplets**:

- **I (Input)** — The full EnterpriseBench directory is the shared input corpus for all evals. No subsetting needed.
- **Q (Question)** — A business question someone in the org would ask.
- **C (Criteria)** — One or more strings outlining what should be present in a correct answer.

Evals live in `dataset.yaml`. Each eval specifies `relevant_sources` (which files/records are needed to answer) — noise is implicit (everything else in EnterpriseBench).

### Key design principle: not all questions are answerable

The system aggregates a lot of noisy data but may **not have all the data**. Some questions should result in the system recognizing it can't answer and suggesting who to ask. For example: "What are the revenue goals for next quarter?" → the data has partial signals (budget discussions in conversations) but no explicit targets, so the system should surface what it found AND suggest asking specific people by name/role.

### Current evals (in dataset.yaml)

| ID | Pattern | Tests |
|----|---------|-------|
| `client_status` | Entity lookup + cross-ref | Find client record, link to rep's conversations |
| `top_products` | Aggregation | Aggregate sales by product/category |
| `support_overview` | Temporal + frequency | Sort by date, count by product |
| `revenue_goals_hitl` | Human-in-the-loop | Recognize missing data, suggest who to ask |
| `it_urgent` | Filtered search | Filter by priority, categorize issues |
| `clients_by_industry` | On-the-fly aggregation | Group by category, compare distributions |
| `multihop_client_contact` | Multi-hop resolution | Client → rep → manager chain |

All criteria are validated against actual data (string-matched and brute-force verified).

**Criteria should be minimal** — only check what was explicitly asked. Don't add criteria for information the question didn't ask for (e.g. don't check "total count" if the question was "which are the top X").

## Evaluation criteria (from Qontext team)

- **Human-in-the-loop**: the system should decide *which human to ask* when it can't answer
- **Scalability**: at least demonstrate direction/capability for scale
- **Input/output format agnosticism**: don't overfit to a specific tool schema (e.g., Slack vs Teams) or input format (e.g., employees.json structure)
- **Consumer agnosticism**: should work with any consumer (Claude Code, HTTP, etc.)

## EnterpriseBench data sources

| File | Domain | Key fields |
|------|--------|------------|
| `employees.json` (36K lines) | HR | name, emp_id, level, salary, leaves, performance, reportees, reports_to |
| `emails.json` (191K lines) | Comms | sender, recipient, subject, body, thread_id, importance |
| `conversations.json` (20K lines) | Collaboration | sender, recipient, date, text |
| `sales.json` (122K lines) | CRM | products, customers, prices, dates |
| `products.json` (12K lines) | CRM | product_id, name, category, price, rating |
| `product_sentiment.json` (95K lines) | CRM | customer reviews and sentiment |
| `customers.json` (631 lines) | CRM | customer directory with order references |
| `customer_support_chats.json` (10K lines) | Support | customer support interactions |
| `clients.json` (7K lines) | Business | client/prospect info with POC status |
| `vendors.json` (4K lines) | Business | vendor information |
| `GitHub.json` (15K lines) | Dev | repos, issues, code content |
| `overflow.json` (225K lines) | Technical Q&A | posts, answers, tags |
| `posts.json` (6K lines) | Social | internal social platform posts |
| `it_tickets.json` (1.5K lines) | IT | tickets with priority and resolution |
| `Policy_Documents/` (25 PDFs) | Compliance | ethics, HR, security policies |
| `Customer_orders/` (271 PDFs) | Finance | invoices, purchase orders |

## Project structure

```
caffeinate/
  CLAUDE.md
  criteria.txt
  dataset.yaml              # eval definitions (I, Q, C triplets)
  EnterpriseBench/
    split_tasks.py          # splits tasks.jsonl into individual files
    tasks/                  # 483 individual task JSON files
    Business_and_Management/
    Collaboration_tools/
    Customer_Relation_Management/
    Enterprise Social Platform/
    Enterprise_mail_system/
    Human_Resource_Management/
    IT_Service_Management/
    Inazuma_Overflow/
    Policy_Documents/
    Workspace/
```
