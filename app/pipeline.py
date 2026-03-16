from pathlib import Path
import pandas as pd


def run_pipeline(input_file: str, output_file: str) -> None:
    df = pd.read_csv(input_file)

    required_columns = {"order_id", "customer", "amount"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing_columns}")

    if (df["amount"] < 0).any():
        raise ValueError("Valores negativos não são permitidos na coluna amount")

    summary = pd.DataFrame(
        {
            "total_sales": [df["amount"].sum()],
            "avg_sales": [df["amount"].mean()],
            "total_orders": [df["order_id"].nunique()],
        }
    )

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output_path, index=False)


if __name__ == "__main__":
    run_pipeline("data/sales.csv", "output/summary.csv")
