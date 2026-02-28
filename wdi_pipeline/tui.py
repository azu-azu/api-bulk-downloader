"""TUI dashboard for wdi-pipeline jobs.

Launch with:
    wdi-pipeline gui --pipeline-dir pipelines/

Keys:
    ↑ / ↓   Move cursor
    q       Quit
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, DataTable, Footer, Header, Input, Label, Select, Switch

from wdi_pipeline.manifest import JobConfig, load_manifest


class EditJobScreen(ModalScreen):
    """Modal dialog for editing a single job's configuration."""

    CSS = """
    EditJobScreen {
        align: center middle;
    }

    #dialog {
        background: $surface;
        border: thick $primary;
        padding: 1 2;
        width: 64;
        height: auto;
    }

    #dialog-title {
        text-style: bold;
        margin-bottom: 1;
        text-align: center;
    }

    #dialog-buttons {
        margin-top: 1;
        align: center middle;
    }

    #dialog-buttons Button {
        margin: 0 1;
    }

    Label {
        margin-top: 1;
    }
    """

    def __init__(
        self,
        manifest_path: Path,
        job: JobConfig,
        default_format: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._manifest_path = manifest_path
        self._job = job
        self._default_format = default_format

    def compose(self) -> ComposeResult:
        job = self._job
        indicator_code = job.connector_params.get("indicator_code", "")
        country_code = job.connector_params.get("country_code", "")
        current_format = job.export.format or self._default_format

        with Vertical(id="dialog"):
            yield Label(f"Edit: {job.job_id}", id="dialog-title")

            yield Label("Enabled")
            yield Switch(value=job.enabled, id="field-enabled")

            yield Label("indicator_code")
            yield Input(value=indicator_code, id="field-indicator_code")

            yield Label("country_code")
            yield Input(value=country_code, id="field-country_code")

            yield Label("filename")
            yield Input(value=job.export.filename, id="field-filename")

            yield Label("format")
            yield Select(
                options=[("csv", "csv"), ("parquet", "parquet")],
                value=current_format,
                allow_blank=False,
                id="field-format",
            )

            # Dynamic sql.params fields
            for key, val in job.sql.params.items():
                yield Label(key)
                yield Input(value=val, id=f"field-param-{key}")

            with Horizontal(id="dialog-buttons"):
                yield Button("Save", variant="primary", id="btn-save")
                yield Button("Cancel", id="btn-cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-cancel":
            self.dismiss(None)
        elif event.button.id == "btn-save":
            self.dismiss(self._collect_values())

    def _collect_values(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        result["enabled"] = self.query_one("#field-enabled", Switch).value
        result["indicator_code"] = self.query_one("#field-indicator_code", Input).value
        result["country_code"] = self.query_one("#field-country_code", Input).value
        result["filename"] = self.query_one("#field-filename", Input).value

        fmt_widget = self.query_one("#field-format", Select)
        fmt_value = fmt_widget.value
        result["format"] = (
            str(fmt_value) if isinstance(fmt_value, str) else self._default_format
        )

        params: dict[str, str] = {}
        for key in self._job.sql.params:
            widget = self.query_one(f"#field-param-{key}", Input)
            params[key] = widget.value
        result["sql_params"] = params

        return result


class PipelineApp(App):
    """TUI dashboard for browsing and editing pipeline jobs."""

    TITLE = "wdi-pipeline"

    CSS = """
    #job-table {
        height: 1fr;
    }

    #main-buttons {
        height: 3;
        align: center middle;
    }

    #main-buttons Button {
        margin: 0 1;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
    ]

    def __init__(self, pipeline_dir: str | Path) -> None:
        super().__init__()
        self._pipeline_dir = Path(pipeline_dir)
        # Each entry: (manifest_path, job, default_format, output_root_str)
        self._rows: list[tuple[Path, JobConfig, str, str]] = []

    def compose(self) -> ComposeResult:
        yield Header()
        yield DataTable(id="job-table")
        with Horizontal(id="main-buttons"):
            yield Button("Edit", id="btn-edit")
            yield Button("Toggle Enabled", id="btn-toggle")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#job-table", DataTable)
        table.add_columns("Enabled", "indicator_code", "filename", "output dir")
        self._load_all_jobs()
        self._refresh_table()

    def _load_all_jobs(self) -> None:
        self._rows = []
        manifest_paths = sorted(self._pipeline_dir.glob("*/manifest.yaml"))
        for manifest_path in manifest_paths:
            try:
                manifest = load_manifest(manifest_path, base_dir=manifest_path.parent)
                with manifest_path.open() as fh:
                    raw = yaml.safe_load(fh)
                default_format = raw.get("defaults", {}).get("export_format", "csv")
                for job in manifest.jobs:
                    self._rows.append(
                        (manifest_path, job, default_format, str(manifest.output_root))
                    )
            except Exception as exc:
                self.notify(f"Error loading {manifest_path.name}: {exc}", severity="error")

        self._rows.sort(
            key=lambda r: (not r[1].enabled, r[1].connector_params.get("indicator_code", ""))
        )

    def _refresh_table(self) -> None:
        table = self.query_one("#job-table", DataTable)
        table.clear()
        for _, job, _, output_root_str in self._rows:
            enabled_str = "✓" if job.enabled else "✗"
            indicator = job.connector_params.get("indicator_code", "")
            filename = f"{job.export.filename}.{job.export.format}"
            table.add_row(enabled_str, indicator, filename, output_root_str)

    def _save_job(self, manifest_path: Path, job_id: str, values: dict[str, Any]) -> None:
        """Write edited values back to manifest YAML."""
        raw = yaml.safe_load(manifest_path.read_text())
        for raw_job in raw.get("jobs", []):
            if raw_job.get("job_id") == job_id:
                raw_job["enabled"] = values["enabled"]

                if "connector_params" not in raw_job:
                    raw_job["connector_params"] = {}
                raw_job["connector_params"]["indicator_code"] = values["indicator_code"]
                raw_job["connector_params"]["country_code"] = values["country_code"]

                if "export" not in raw_job:
                    raw_job["export"] = {}
                raw_job["export"]["filename"] = values["filename"]
                raw_job["export"]["format"] = values["format"]

                if "sql" not in raw_job:
                    raw_job["sql"] = {}
                if "params" not in raw_job["sql"]:
                    raw_job["sql"]["params"] = {}
                for k, v in values["sql_params"].items():
                    raw_job["sql"]["params"][k] = v

                break

        manifest_path.write_text(yaml.dump(raw, allow_unicode=True, sort_keys=False))

    def on_button_pressed(self, event: Button.Pressed) -> None:
        table = self.query_one("#job-table", DataTable)
        cursor_row = table.cursor_row

        if not self._rows or cursor_row < 0 or cursor_row >= len(self._rows):
            return

        manifest_path, job, default_format, _output_root_str = self._rows[cursor_row]

        if event.button.id == "btn-toggle":
            job.enabled = not job.enabled

            raw = yaml.safe_load(manifest_path.read_text())
            for raw_job in raw.get("jobs", []):
                if raw_job.get("job_id") == job.job_id:
                    raw_job["enabled"] = job.enabled
                    break
            manifest_path.write_text(yaml.dump(raw, allow_unicode=True, sort_keys=False))

            self._refresh_table()

        elif event.button.id == "btn-edit":
            def on_edit_result(result: dict[str, Any] | None) -> None:
                if result is None:
                    return

                # Update in-memory job config
                job.enabled = result["enabled"]
                job.connector_params["indicator_code"] = result["indicator_code"]
                job.connector_params["country_code"] = result["country_code"]
                job.export.filename = result["filename"]
                job.export.format = result["format"]
                for k, v in result["sql_params"].items():
                    job.sql.params[k] = v

                self._save_job(manifest_path, job.job_id, result)

                self._refresh_table()

            self.push_screen(
                EditJobScreen(manifest_path, job, default_format),
                on_edit_result,
            )
