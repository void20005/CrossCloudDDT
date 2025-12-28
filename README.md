# CrossCloudDDT (Data Driven Testing)

**CrossCloudDDT** is a robust, Data-Driven Framework for End-to-End Testing across **Salesforce Sales Cloud** and **Marketing Cloud**.

> **Philosophy**: Data defines the Cross-Cloud Scenario. 
> Instead of hardcoding test data, you define complex hierarchies in CSVs, and the framework orchestrates them across clouds, handling dependencies, consents, and integrations.

---

## 🎯 Core Features

### 1. 🏭 Generic Data Factory
The engine (`src/data_factory.py`) is object-agnostic. It parses CSV headers to dynamically construct Salesforce payloads.
*   **Smart Linking**: Automatically resolves dependencies (e.g., link Opportunity to a just-created Account).
*   **Date Math**: Supports relative dates (`CloseDate__date`: `30` → +30 days from today).
*   **Idempotency (Upsert)**: Can safely re-run scenarios. Updates existing records instead of creating duplicates.

### 2. 🧠 Intelligent Automation
*   **Consent Management**: Automatically handles GDPR/TCPR artifacts (`Individual`, `ContactPointTypeConsent`) created by Salesforce flows.
*   **Cascade Cleanup**: Deletes complex hierarchies (`Account` → `Individual` → `CPTC`) to preventing data pollution.
*   **Return Fields**: Captures auto-generated values (like `AssetId` created by a trigger on `Vehicle`) for use in subsequent steps.

### 3. 🏗️ Handler Architecture
Uses a Strategy Pattern (`src/handlers/`) to manage object-specific complexities (Side Effects, Triggers) without cluttering the core engine.

---

## 🚀 Quick Start

### 1. Installation
```bash
pip install -r requirements.txt
# Create .env.qa with your credentials
```

### 2. Run a Scenario
Generates data defined in `data/lost_sale/*.csv`:
```bash
python main.py --scenario lost_sale --upsert
```

### 3. Cleanup Data
Smartly identifies and removes test data created by the scenario:
```bash
python main.py --scenario lost_sale --delete
```

### 4. Pattern-Based Cleanup
Delete records by pattern (uses handlers for cascade logic):
```bash
# Delete Accounts matching pattern (with cascade: CPTC -> Account -> Individual)
python main.py --clean "LostSale%"

# Delete other objects by pattern
python main.py --clean "Test%" --object Opportunity
python main.py --clean "%SMOT%" --object Vehicle
```

---

## 📚 Data Preparation Guide

The framework processes CSV files in **alphabetical order**. File names determine the Salesforce Object.

**Example Structure:**
*   `01_Account.csv` (Creates Accounts)
*   `02_Vehicle.csv` (Creates Vehicles linked to Accounts)
*   `04_AssetAccountParticipant.csv` (Links Assets to Accounts)

### Special Syntax

| Syntax | Description | Example |
| :--- | :--- | :--- |
| `_BaseName` | **Unique Alias**. Used to reference this record in other files. | `MyTestAccount` |
| `_Ref:FieldName` | **Reference**. Injects ID of a previously created record. | `_Ref:MyTestAccount` |
| `_Ref:Alias.Field` | **Deep Reference**. Injects a specific field from an alias. | `_Ref:MyVehicle.AssetId` |
| `_Return:Field` | **Capture**. Asks framework to fetch this field after creation. | `_Return:AssetId` |
| `Field__date` | **Relative Date**. Integer days from today. | `30` (in `CloseDate__date`) |
| `_Config` | **Internal**. Config columns (not sent to Salesforce). | `_EmailConsent` |

### Advanced Usage Examples

#### 1. Capturing Side-Effects (Return Fields)
Creating a `Vehicle` often triggers a Flow that creates an `Asset`. To use that `AssetId` later:
*   **01_Vehicle.csv**: `_Return:AssetId` (Fetches AssetId after Vehicle creation).
*   **02_Participant.csv**: `_Ref:VehicleAlias.AssetId` (Uses the fetched AssetId).

#### 2. Managing Consents (Person Accounts)
To explicitly control GDPR consents:
*   Add columns `_EmailConsent`, `_SMSConsent` (`OptIn`/`OptOut`) to your **Account** CSV.
*   The framework automatically waits for `Individual` & `CPTC` creation and updates them.

#### 3. Solving Circular Dependencies (`_update` Suffix)
For objects with mutual references (e.g., `BranchUnit` ↔ `BranchUnitBusinessMember`):
*   **01_BranchUnit.csv**: Create without optional circular reference (e.g., `PrimaryAftersalesContact` left blank).
*   **02_BranchUnitBusinessMember.csv**: Create with reference to `BranchUnit`.
*   **03_BranchUnit_update.csv**: Update `BranchUnit` to add `PrimaryAftersalesContact`.

**Convention**: Files with `_update` in the name automatically trigger **UPSERT mode**, allowing you to update records created earlier in the scenario.

---

## 🏗️ Architecture & Extensions

The project uses a **Handler Pattern** (`src/handlers/`) to decouple logic and manage **Salesforce Side Effects**.

### A. Default (`BaseHandler`)
For standard objects without complex side effects (e.g., `Opportunity`, `Task`). 
*   **Usage**: Zero code. Just add a CSV.

### B. API Compliance (`other_handlers.py`)
For objects with **Immutable Fields** (fields that cannot be updated after creation due to API/Schema constraints).
*   **Usage**: Create a simple handler defining `get_immutable_fields()`.
*   *Example*: `VehicleDefinition` forbids updating `ProductId`.

### C. Side-Effect Management (Custom Handlers)
For objects that act as "Trigger Roots", creating hidden records via Flows/Triggers.
*   **Scenario**: Deleting `Account` leaves orphaned `Individual` records.
*   **Solution**: `AccountHandler` overrides `delete_records` to perform **Cascade Deletion** (`CPTC` → `Account` → `Individual`).
*   **Usage**: Extend `BaseHandler` in a new file (e.g. `src/handlers/my_handler.py`).

### D. Handler Registry
Handlers are registered in `HANDLER_REGISTRY` dictionary at the top of `data_factory.py`:
```python
HANDLER_REGISTRY = {
    'Account': AccountHandler,
    'Vehicle': VehicleHandler,
    # Add new handlers here
}
```
Objects not in the registry automatically use `BaseHandler`.

---

## 📂 Project Structure

```text
cross-cloud-ddt/
├── main.py                  # Entry Point (CLI)
├── src/
│   ├── data_factory.py      # Core Engine
│   └── handlers/            # Object-specific strategies
│       ├── base_handler.py
│       ├── account_handler.py
│       └── ...
├── data/                    # SCENARIOS (CSV definitions)
│   ├── lost_sale/
│   └── ...
└── tests/                   # Pytest suite
```
