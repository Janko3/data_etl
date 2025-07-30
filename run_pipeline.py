from pipeline.bronze import process_bronze
from pipeline.silver import process_silver
from pipeline.gold import process_gold


def main():
    print("Starting pipeline")

    try:
        print("Bronze Layer")
        process_bronze()
        print("Bronze layer completed.")

        print("Silver Layer")
        process_silver()
        print("Silver layer completed.")

        print("Gold Layer")
        process_gold()
        print("Gold layer completed.")

        print("Pipeline completed successfully.")

    except Exception as e:
        print(f"Pipeline failed: {e}")

if __name__ == "__main__":
    main()
