#!/usr/bin/env python3
"""
Generate EnterpriseBench2 from EnterpriseBench.
Same data, different field names: semantic renames + snake_case → camelCase.
Tests that the qontext layer doesn't hardcode field names.
"""

import json
import os
import re
import shutil
from pathlib import Path

SRC = Path("EnterpriseBench")
DST = Path("EnterpriseBench2")

# Renames that are ambiguous across files — keyed by filename stem
FILE_SCOPED_RENAMES = {
    "employees": {
        "category": "department",
        "description": "roleDescription",
    },
}

# Semantic renames: original_key → new_key (already in camelCase)
SEMANTIC_RENAMES = {
    # employees.json
    "emp_id": "employeeId",
    "Name": "fullName",
    "Experience": "yearsOfExperience",
    "Level": "jobLevel",
    "DOJ": "startDate",
    "DOL": "endDate",
    "Salary": "annualSalary",
    "Total Casual Leaves": "totalCasualLeaves",
    "Remaining Casual Leaves": "remainingCasualLeaves",
    "Total Sick Leaves": "totalSickLeaves",
    "Remaining Sick Leaves": "remainingSickLeaves",
    "Total Vacation Leaves": "totalVacationLeaves",
    "Remaining Vacation Leaves": "remainingVacationLeaves",
    "Total Leaves Taken": "totalLeavesTaken",
    "Age": "age",
    "Performance Rating": "performanceScore",
    "Marital Status": "maritalStatus",
    "Gender": "gender",
    "reports_to": "managerId",
    "is_valid": "isActive",

    # clients.json / vendors.json
    "client_id": "clientId",
    "business_name": "companyName",
    "industry": "domain",
    "business_type": "companyType",
    "contact_person_id": "contactPersonId",
    "contact_person_name": "contactPersonName",
    "contact_email": "contactEmail",
    "phone_number": "phoneNumber",
    "registered_address": "registeredAddress",
    "tax_id": "taxId",
    "monthly_revenue": "monthlyRevenue",
    "onboarding_date": "onboardingDate",
    "current_POC_product": "currentPocProduct",
    "POC_status": "pocStatus",
    "engagement_description": "engagementDescription",
    "business_representative_employee": "businessRepEmployeeId",
    "relationship_description": "relationshipDescription",
    "management_representative_employee": "managementRepEmployeeId",

    # conversations.json
    "conversation_id": "conversationId",
    "sender_emp_id": "senderEmployeeId",
    "recipient_emp_id": "recipientEmployeeId",

    # sales.json
    "product_id": "productId",
    "discounted_price": "salePrice",
    "actual_price": "listPrice",
    "discount_percentage": "discountPercent",
    "customer_id": "customerId",
    "Date_of_Purchase": "purchaseDate",
    "sales_record_id": "salesRecordId",

    # products.json
    "product_name": "productName",
    "about_product": "productDescription",

    # product_sentiment.json
    "review_content": "reviewText",
    "review_date": "reviewDate",
    "sentiment_id": "sentimentId",

    # customers.json
    "customer_name": "customerName",
    "invoice_paths": "invoicePaths",
    "purchase_order_paths": "purchaseOrderPaths",
    "shipping_order_paths": "shippingOrderPaths",

    # customer_support_chats.json
    "interaction_date": "interactionDate",
    "chat_id": "chatId",

    # emails.json
    "email_id": "emailId",
    "thread_id": "threadId",
    "sender_email": "senderEmail",
    "sender_name": "senderName",
    "sender_emp_id": "senderEmployeeId",
    "recipient_email": "recipientEmail",
    "recipient_name": "recipientName",
    "recipient_emp_id": "recipientEmployeeId",

    # posts.json / overflow.json
    "PostTypeId": "postTypeId",
    "AcceptedAnswerId": "acceptedAnswerId",
    "ParentId": "parentId",
    "Score": "score",
    "ViewCount": "viewCount",
    "Body": "body",
    "Title": "title",
    "ContentLicense": "contentLicense",
    "FavoriteCount": "favoriteCount",
    "CreationDate": "createdAt",
    "LastActivityDate": "lastActivityAt",
    "LastEditDate": "lastEditAt",
    "LastEditorUserId": "lastEditorUserId",
    "OwnerUserId": "ownerUserId",
    "Tags": "tags",
    "employee_id": "employeeId",
    "employee_Name": "employeeName",

    # it_tickets.json
    "raised_by_emp_id": "raisedByEmployeeId",
    "assigned_date": "assignedDate",
    "Issue": "issueDescription",
    "Resolution": "resolutionDescription",

    # GitHub.json
    "repo_name": "repositoryName",
    "creation_date": "createdAt",

    # GitHub.json nested issues
    "created_at": "createdAt",
}


def snake_to_camel(name: str) -> str:
    parts = name.split("_")
    return parts[0].lower() + "".join(p.capitalize() for p in parts[1:])


def rename_key(key: str, file_overrides: dict) -> str:
    if key in file_overrides:
        return file_overrides[key]
    if key in SEMANTIC_RENAMES:
        return SEMANTIC_RENAMES[key]
    if "_" in key:
        return snake_to_camel(key)
    return key


def transform(obj, file_overrides: dict):
    if isinstance(obj, dict):
        return {rename_key(k, file_overrides): transform(v, file_overrides) for k, v in obj.items()}
    if isinstance(obj, list):
        return [transform(item, file_overrides) for item in obj]
    return obj


def process_json(src_path: Path, dst_path: Path):
    stem = src_path.stem
    file_overrides = FILE_SCOPED_RENAMES.get(stem, {})
    with open(src_path, "r") as f:
        data = json.load(f)
    transformed = transform(data, file_overrides)
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dst_path, "w") as f:
        json.dump(transformed, f, indent=2, ensure_ascii=False)
    print(f"  {src_path.relative_to(SRC)} -> {dst_path.relative_to(DST)}")


def main():
    if DST.exists():
        shutil.rmtree(DST)
    DST.mkdir()

    skip_dirs = {"tasks"}

    for root, dirs, files in os.walk(SRC):
        root_path = Path(root)
        rel = root_path.relative_to(SRC)

        dirs[:] = [d for d in dirs if d not in skip_dirs]

        for fname in files:
            src_file = root_path / fname
            dst_file = DST / rel / fname

            if fname.endswith(".json"):
                process_json(src_file, dst_file)
            elif fname == "tasks.jsonl":
                continue
            else:
                dst_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_file, dst_file)
                print(f"  (copy) {(rel / fname)}")


if __name__ == "__main__":
    main()
