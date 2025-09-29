# Ancestry categories

The GWAS‑Studio platform uses a **standardised list of ancestry (population) categories**.
Each category has a short, machine‑friendly code (the *shortcut*) that can be stored in the database or used in API calls, while the full description is shown to users.

## Ancestry shortcut table

| Shortcut | Full description                                                   |
|----------|--------------------------------------------------------------------|
| **AUS**  | Aboriginal Australian                                              |
| **AFA**  | African American or Afro‑Caribbean                                 |
| **AFR**  | African unspecified                                                |
| **ASN**  | Asian unspecified                                                  |
| **CAS**  | Central Asian                                                      |
| **EAS**  | East Asian                                                         |
| **EUR**  | European                                                           |
| **MDE**  | Greater Middle Eastern (Middle Eastern, North African, or Persian) |
| **AMR**  | Hispanic or Latin American                                         |
| **ISL**  | Icelandic                                                          |
| **NR**   | Not reported / unknown                                             |
| **NAM**  | Native American                                                    |
| **OCE**  | Oceanian                                                           |
| **OTH**  | Other                                                              |
| **ADM**  | Other admixed ancestry                                             |
| **SAS**  | South Asian                                                        |
| **SEA**  | South East Asian                                                   |
| **SAF**  | Sub‑Saharan African                                                |

### How to use the table
* **Database / API** – store the *shortcut* (e.g., `EUR`) and map it to the full description when presenting data to users.
* **User interfaces** – show the full description in dropdowns or tooltips; the shortcut can be used for filtering or as a compact label.
* **Validation** – accept either the shortcut or the full description, then normalise to the shortcut for internal consistency.

---

*The list is version‑controlled; any addition or change should be reflected both in `src/gwasstudio/config/config.yaml` (under `ancestry_groups`) and in this documentation.*

Source: [https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5815218/table/Tab1/?report=objectonly](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5815218/table/Tab1/?report=objectonly)
