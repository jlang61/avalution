# Revision Manager Spec

### Firewood Revision Manager
- **Files to focus on in:**
  - [Firewood `manager.rs`](https://github.com/ava-labs/firewood/blob/5e9db421cd195740d2269aea49548e97c7104f3b/firewood/src/manager.rs#L20)
  - [Firewood `db.rs`](https://github.com/ava-labs/firewood/blob/5e9db421cd195740d2269aea49548e97c7104f3b/firewood/src/db.rs#L11)
- Focus more on this after we finish implementing:
  - Free list
  - Serialization
  - On-disk MerkleDB
- [**MerkleDB README**](https://github.com/ava-labs/avalanchego/tree/master/x/merkledb): Has more in depth information about Views

---

## Preliminary Overview

- **MerkleDB Node Changes**:  
  If any node changes in a MerkleDB instance, the root ID will change.  
  - This happens because changing a node’s ID changes its parent’s ID, and so on, up to the root.

- **Root ID**:  
  - Serves as an identifier of the DB’s state.  
  - States associated with a given root ID are called a **revision**.  
  - Two MerkleDB instances are **equivalent** if their root IDs are the same.  
  - Saying that an instance is “at” a given **revision** or **root ID** means the same thing.

---

## Views in MerkleDB

- **View**:  
  - A **view** is a proposal to modify the MerkleDB.
  - If a view is **committed**, its changes are applied to the MerkleDB.
  - **Committed data** can no longer be changed.

- **View Relationships**:
  - If `view1` is built on `db`, it contains everything in `db` plus its own changes.
  - If `view3` is built on `view1`, it contains all the changes of `view1` plus its own modifications.

- **Commitment Rules**:
  - A view can only be committed if its **parent** is the **db** (e.g., `view1` must be committed before `view3`).
  - If a **sibling view** is committed, the other siblings and their descendants become **invalid**.

- **Example**:
    ```
        db
      /    \
    view1  view2
      |
    view3
    ```
  - If `view2` is committed:
    - `view1` and `view3` are **invalidated** and can no longer be read or committed.
