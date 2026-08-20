# Senior Forward Deployed Engineer — ZoomInfo

- **Location:** Remote
- **Type:** Full time
- **Posted:** 27 July 2026
- **Req ID:** JR107911
- **US base salary:** $143,500–$225,500 (location, qualifications, and other compensation such as bonus, commission, or equity may apply)

ZoomInfo (NASDAQ: GTM) is a go-to-market intelligence platform. This role sits in a **new Forward Deployed Engineering function** that ZoomInfo is still defining.

## The role

Help define how FDE operates: how engagements run, what the deliverables are, what the playbook is, and what scales. Support already exists (data, product, infrastructure, executive sponsorship). The job is to bring that to life in front of customers and turn early wins into a repeatable model.

Embed with **strategic accounts** — large enterprises with complex data needs, often in financial services, insurance, technology, and other globally distributed industries. Work alongside their teams, then design and deliver bespoke intelligence applications that combine ZoomInfo third-party data with the customer's first-party data.

Own engagements **end-to-end**: discovery through deployment, stakeholder presentations through production code. Work with data and product (and more senior FDEs on larger accounts) across ZoomInfo's data foundation: company intelligence, contact data, buying signals, intent data, and specialized vertical datasets — assembled into purpose-built applications for each customer's personas and workflows.

ZoomInfo's data foundation (as stated in the posting): 500M+ professional profiles, 100M+ company records, intent signals, and vertical datasets spanning financial filings, insurance, commercial fleet, and more.

## What engagements typically look like

Most engagements combine some of:

- **Entity resolution at scale** — reconciling legal-entity hierarchies (D&B, tax IDs, company-house registrations) with how customers actually go to market. Multinationals with hundreds of legal entities collapsing to a single GTM record.
- **Hierarchy management** — one-to-one matching across regions, parent-child linkage gaps, orphaned-account disposition, white space on top of clean parent IDs.
- **Location-level precision** — moving customers off HQ-level enrichment so geo-based sales teams see local firmographics instead of global rollups.
- **Automated, no-human-in-the-loop logic** — entity suppression, disposition-based matching, orchestration rules for inactive entities, parent linkages, and white space alerts.
- **Data warehouse as the operating layer** — moving hierarchy work out of CRM (Salesforce cannot do this at scale) into Snowflake or BigQuery, via API or data cube depending on the workflow.
- **Buying-group filtering** — persona-density criteria across hierarchies (example in the posting: 5,600 Disney legal entities → 31 actionable targets).

Two reference engagements from the posting:

1. **Global infrastructure customer:** 1.8M records, 300K flagged unmatched, data team of one. An automated domain-validation pipeline reframed a "coverage gap" as a data-quality program: 175K inactive sites, 30K redirects, 65K real opportunities.
2. **Enterprise planning platform:** 120K Salesforce accounts, broken hierarchies, near-zero field-leader confidence in enrichment. Underlying data was accurate; matching was wrong. Custom disposition logic on a 10K-account priority sample produced 7,444 high-confidence matches at 98–99% accuracy, validated before scaling.

Work moves between data engineering, applied product development, and stakeholder management — often in the same week.

## What you build

Every engagement uses a consistent service architecture: **three pillars** stacked, and **five capability areas** assembled into the deliverable.

### Three pillars

1. **Data Foundation** — golden reference matching, persistent IDs, unified entity profiles across the customer's first-party systems.
2. **Data Management** — business-specific logic that turns the foundation into something the customer can go to market with: customer definitions, account models, entity resolution.
3. **Activation** — TAM to SAM to SOM, fit scoring, in-market signals.

### Five capability areas

Most engagements use at least three. Diagnose with your manager and more senior FDEs which the customer actually needs; take increasing ownership of that diagnosis.

1. **Data Foundation Development** — Match every record across CRM, ERP, billing, and marketing systems to a golden reference dataset. Custom disposition logic, domain validation, marketability classification, and legal-entity crosswalks. Output: a single persistent ID linking every system.
2. **Account Architecture & Entity Resolution** — Define what an account means for that business (address-based, country, HQ, ultimate-parent rollup, or hybrid). Automated logic that enforces it: duplicate resolution, inactive-entity disposition, hierarchy linkages.
3. **TAM Development & White Space Discovery** — Complete addressable market against ICP criteria; suppress existing customers via ultimate-parent rollup; surface white space inside customer hierarchies; apply buying-group filters to reduce entity volume to targeted pipeline.
4. **Account Fit Scoring & In-Market Signals** — Custom fit models from historical win/loss; evergreen signals (executive moves, leadership changes, funding rounds) and tailored ones (intent topic spikes, senior job postings, technology displacement).
5. **Ongoing Governance & Automation** — Match orchestration rules, enrichment segmentation, CRM field locking, and warehouse integration (Snowflake, BigQuery) that keep the foundation clean as the business evolves.

## What you do

- **Own strategic customer engagements end-to-end.** Primary technical point of contact. Discovery with data engineers, sales operations, and revenue stakeholders. Diagnose the real problem (a "coverage gap" is often a marketability problem; "we don't trust the data" is often a matching problem). Scope, design, build, deploy in the customer's environment, and stay accountable for the outcome.
- **Bridge technical and business audiences.** Whiteboard matching architecture with data engineering; walk a sales-ops director through disposition codes; develop the ability to present ROI and strategy to executives.
- **Contribute to the FDE playbook.** Document discovery frameworks, engagement phases, integration patterns, deliverable templates, and success metrics. Extract what is repeatable from each account and feed field learnings back to product, engineering, and data.
- **Drive stickiness and expansion.** Identify new use cases, personas, datasets, and displacement opportunities against incumbent providers.

## What they are looking for

- **High ownership, comfort with ambiguity.** The function is still being defined. Judgment calls with incomplete information; operate where process does not yet exist.
- **Software and data engineering fundamentals.** Production-quality code. Proficient in Python and SQL. Comfortable in cloud data warehouses (Snowflake, BigQuery, Databricks, or similar). Built or substantially worked with data pipelines, entity matching or deduplication, API integrations, and applications users depend on daily. API tooling (GraphQL, REST, Postman, JWT, OAuth) is a plus — navigate and integrate quickly, deep expertise not required. Fluent in LLM-based development environments (Claude Code, Codex, or similar) — treated as core tools, not a nice-to-have.
- **Customer-facing communication.** Prior technical customer work (solutions engineering, consulting, TAM, or FDE-style). Synthesize data needs for a business audience and discuss matching architecture with data engineering in the same week. Navigate enterprise stakeholders: sales ops, IT, marketing ops, finance, and revenue leadership.
- **Go-to-market data familiarity (preferred, not required).** B2B data; CRM (Salesforce, HubSpot); enrichment/orchestration (RingLead, Clearbit, Demandbase); firmographic enrichment, entity resolution, hierarchy management, TAM modeling, intent data, account-based prospecting.

## Why the function exists (from the posting)

Driving data consumption and growth across ZoomInfo's strategic accounts is a stated company priority. Working prototypes, validated demand, and executive sponsorship already exist. The data, product, and infrastructure teams are in place. The hire is expected to execute and help grow the function.
