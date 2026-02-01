from methods.database import Database

import pandas as pd
import matplotlib.pyplot as plt


class Plot:
    @staticmethod
    def export_asia_report_flights_pie(
        db_parameters,
        output_png_path,
        top_n,
        also_export_excel,
        output_excel_path,
    ):

        sql = """
            SELECT
                airline_name,
                COALESCE(total_flights_to_asia, 0) AS total_flights_to_asia
            FROM asia_report
            WHERE COALESCE(total_flights_to_asia, 0) > 0
            ORDER BY total_flights_to_asia DESC;
        """

        conn = Database.get_connection(db_parameters)

        try:
            df = pd.read_sql(sql, conn)

            if df.empty:
                raise ValueError(
                    "asia_report returned 0 rows (or total_flights_to_asia is all 0)."
                )

            # Use airline_name directly for labels
            df["label"] = df["airline_name"].fillna("(unknown)").str.strip()

            # Top N airlines + Other
            df_top = df.head(top_n).copy()
            df_rest = df.iloc[top_n:].copy()

            labels = df_top["label"].tolist()
            values = df_top["total_flights_to_asia"].astype(float).tolist()

            # Combine remaining airlines into "Other"
            other_sum = float(df_rest["total_flights_to_asia"].sum()) if not df_rest.empty else 0.0
            if other_sum > 0:
                labels.append("Other")
                values.append(other_sum)

            # -------------------------------
            # FIX: Force China Southern + Other to different colors
            # -------------------------------
            colors = []
            for lab in labels:
                if lab == "China Southern Airlines":
                    colors.append("red")          # China Southern = red
                elif lab == "Other":
                    colors.append("blue")         # Other = blue
                else:
                    colors.append(None)           # Default Matplotlib colors

            # Create pie chart
            plt.figure(figsize=(10, 8))
            default_colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
            colors = [default_colors[i % len(default_colors)] for i in range(len(labels))]

            if "China Southern Airlines" in labels and "Other" in labels:
                cs_index = labels.index("China Southern Airlines")
                other_index = labels.index("Other")

                if colors[cs_index] == colors[other_index]:
                    colors[other_index] = default_colors[(other_index + 1) % len(default_colors)]

            # Pie chart (same as before, just add colors=colors)
            plt.pie(
                values,
                labels=labels,
                autopct="%1.1f%%",
                startangle=90,
                colors=colors
            )

            plt.title(
                f"Asia Report: Total Flights by Airline Name (Top {top_n}"
                + (" + Other" if other_sum > 0 else "")
                + ")"
            )

            plt.tight_layout()

            # Save chart
            plt.savefig(output_png_path, dpi=200)
            plt.close()

            # Optional Excel export
            if also_export_excel:
                df.to_excel(output_excel_path, index=False)

            print(f"Pie chart saved to: {output_png_path}")

            if also_export_excel:
                print(f"Excel export saved to: {output_excel_path}")

        finally:
            conn.close()
