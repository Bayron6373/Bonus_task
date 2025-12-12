# Population Data Scraper and Kafka Producer

This project scrapes population data of countries from Wikipedia, cleans it, and sends it as JSON messages to a Kafka topic. The cleaned data is also saved locally as a CSV file.

---

## **Data Source**

- **URL:** [List of countries and dependencies by population](https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population)
- **Description:**  
  The dataset contains population information for all countries and dependencies, including:
  - Country name
  - Population count
  - Percentage of world population
  - Date of data
  - Source (official or from the United Nations)

---

## **Data Cleaning Steps**

1. **Remove missing or invalid values**  
   Rows with missing `Date` or `Population` are removed.

2. **Drop unnecessary columns**  
   The `Notes` column is removed.

3. **Convert numbers and dates**  
   - `Population` is converted to integers (removing commas).  
   - `% of world` is converted to float (removing `%`).  
   - `Date` is converted to proper datetime format.

4. **Rename columns for clarity**

| Original Column | New Column       |
|-----------------|----------------|
| Location        | country        |
| Population      | population     |
| % ofworld       | percent_world  |
| Date            | date           |
| Source (...)    | source         |

5. **Normalize strings**  
   - Trim whitespace  
   - Convert country names to lowercase  

---

## **Kafka Producer**

- **Topic Name:** `bonus_22B030147`
- Each row is sent as a JSON message.
- **Sample message:**

```json
{
  "id": 0,
  "country": "china",
  "population": 1402112000,
  "percent_world": 18.0,
  "date": "2023-01-01",
  "source": "United Nations"
}
