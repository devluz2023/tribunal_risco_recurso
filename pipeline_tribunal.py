import os
import numpy as np
import pandas as pd
from datetime import timedelta
import great_expectations as gx
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score
from sklearn.model_selection import train_test_split
from evidently.legacy.report import Report
from evidently.legacy.metric_preset import DataDriftPreset

from feast import Entity, Field, FeatureView, FileSource, ValueType
from metaflow import FlowSpec, step


# ==========================================
# 1. VALIDAÇÃO DE DADOS (GREAT EXPECTATIONS)
# ==========================================
def validar_dados_brutos(df: pd.DataFrame):
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas(name="pandas_datasource")
    asset = datasource.add_dataframe_asset(name="processos_asset")
    batch_definition = asset.add_batch_definition_whole_dataframe(
        name="processos_batch"
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    context.suites.add(gx.ExpectationSuite(name="tribunal_suite"))
    validator = context.get_validator(
        batch=batch, expectation_suite_name="tribunal_suite"
    )

    validator.expect_column_values_to_not_be_null("numero_recursos_anteriores")
    validator.expect_column_values_to_be_between(
        "valor_causa", min_value=0.0, max_value=1e9
    )
    validator.expect_column_values_to_be_in_set(
        "risco_recurso", ["baixo", "medio", "alto"]
    )

    validation_result = validator.validate()
    if not validation_result["success"]:
        raise ValueError("Falha na validação de qualidade de dados do Great Expectations!")
    print("Validação de dados concluída com sucesso via Great Expectations.")


# ==========================================
# 2. PIPELINE DE MLOPS (METAFLOW + KUBEFLOW)
# ==========================================
class TribunalKubeflowFlow(FlowSpec):

    @step
    def start(self):
        print("Iniciando pipeline MLOps para Classificação de Processos Jurídicos.")
        self.next(self.generate_and_validate_data)

    @step
    def generate_and_validate_data(self):
        np.random.seed(42)
        n_samples = 100000

        # Simulação de dados (Faker / Pandas)
        self.raw_df = pd.DataFrame({
            "numero_processo": [f"PROC-{i:06d}" for i in range(n_samples)],
            "valor_causa": np.random.exponential(scale=50000, size=n_samples),
            "tempo_tramitacao": np.random.poisson(lam=18, size=n_samples) + 1,
            "tipo_processo": np.random.choice(
                ["civel", "trabalhista", "tributario"],
                size=n_samples,
                p=[0.5, 0.3, 0.2],
            ),
            "numero_recursos_anteriores": np.random.poisson(lam=0.8, size=n_samples),
            "risco_recurso": np.random.choice(
                ["baixo", "medio", "alto"], size=n_samples, p=[0.85, 0.12, 0.03]
            ),
        })

        validar_dados_brutos(self.raw_df)
        self.next(self.feature_engineering)

    @step
    def feature_engineering(self):
        df = self.raw_df.copy()

        # Engenharia de Features
        df["valor_causa_log"] = np.log1p(df["valor_causa"])
        df["tempo_tramitacao_meses"] = df["tempo_tramitacao"].astype(float)
        df["ratio_recursos_tempo"] = (
            df["numero_recursos_anteriores"] + 1
        ) / df["tempo_tramitacao_meses"]

        tipo_mapping = {"civel": 0, "trabalhista": 1, "tributario": 2}
        df["tipo_processo_encoded"] = df["tipo_processo"].map(tipo_mapping)

        risk_mapping = {"baixo": 0, "medio": 1, "alto": 2}
        df["target"] = df["risco_recurso"].map(risk_mapping)

        df["event_timestamp"] = pd.Timestamp.now()
        df["created_timestamp"] = pd.Timestamp.now()

        # Salvando Parquet para o Feast
        os.makedirs("data", exist_ok=True)
        parquet_path = "data/processos_features.parquet"
        df[[
            "numero_processo",
            "valor_causa_log",
            "tempo_tramitacao_meses",
            "ratio_recursos_tempo",
            "tipo_processo_encoded",
            "numero_recursos_anteriores",
            "event_timestamp",
            "created_timestamp",
        ]].to_parquet(parquet_path)

        self.processed_df = df
        self.next(self.train_model)

    @step
    def train_model(self):
        features = [
            "valor_causa_log",
            "tempo_tramitacao_meses",
            "ratio_recursos_tempo",
            "tipo_processo_encoded",
            "numero_recursos_anteriores",
        ]
        X = self.processed_df[features]
        y = self.processed_df["target"]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        # Integração MLflow
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001"))
        mlflow.set_experiment("tribunal_risco_recurso")

        with mlflow.start_run() as run:
            n_estimators = 100
            max_depth = 10

            clf = RandomForestClassifier(
                n_estimators=n_estimators,
                max_depth=max_depth,
                class_weight="balanced",
                random_state=42,
            )
            clf.fit(X_train, y_train)

            preds = clf.predict(X_test)
            f1_macro = f1_score(y_test, preds, average="macro")

            mlflow.log_param("n_estimators", n_estimators)
            mlflow.log_param("max_depth", max_depth)
            mlflow.log_metric("f1_macro", f1_macro)
            mlflow.sklearn.log_model(
                clf,
                name="random_forest_model",
                skops_trusted_types=["sklearn.tree._tree.Tree"],
            )

            # Monitoramento de Data Drift com Evidently
            drift_report = Report(metrics=[DataDriftPreset()])
            drift_report.run(reference_data=X_train, current_data=X_test)
            
            report_path = "data/evidently_drift_report.html"
            drift_report.save_html(report_path)
            mlflow.log_artifact(report_path)

        self.next(self.end)

    @step
    def end(self):
        print("Pipeline executado com sucesso e integrado ao Kubeflow e MLflow!")


if __name__ == "__main__":
    TribunalKubeflowFlow()