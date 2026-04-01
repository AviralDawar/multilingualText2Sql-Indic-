#!/usr/bin/env python3
import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


def load_scores(csv_path: Path, include_unknown: bool) -> list[dict]:
    rows = []
    with csv_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not include_unknown and row["lang"] == "Unknown":
                continue
            rows.append(
                {
                    "db": row["db"],
                    "lang": row["lang"],
                    "pair_id": row["pair_id"],
                    "score": float(row["comet_score"]),
                }
            )
    return rows


def mean(values: list[float]) -> float:
    return sum(values) / len(values)


def stddev(values: list[float], mu: float) -> float:
    return math.sqrt(sum((value - mu) ** 2 for value in values) / len(values))


def empirical_knee(points: list[tuple[float, float]]) -> int:
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]

    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)

    norm_points = []
    for x_value, y_value in points:
        x_norm = (x_value - min_x) / (max_x - min_x) if max_x > min_x else 0.0
        y_norm = (y_value - min_y) / (max_y - min_y) if max_y > min_y else 0.0
        norm_points.append((x_norm, y_norm))

    x1, y1 = norm_points[0]
    x2, y2 = norm_points[-1]
    dx, dy = x2 - x1, y2 - y1
    denom = math.sqrt(dx * dx + dy * dy)
    if denom == 0:
        return 0

    best_index = 0
    best_distance = -1.0
    for index, (x_value, y_value) in enumerate(norm_points):
        distance = abs(dx * (y1 - y_value) - (x1 - x_value) * dy) / denom
        if distance > best_distance:
            best_distance = distance
            best_index = index
    return best_index


def write_metrics_csv(output_path: Path, metrics: list[dict]) -> None:
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "k",
            "tau",
            "flagged_n",
            "total_n",
            "review_rate",
            "review_pct",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(metrics)


def write_breakdown_csv(output_path: Path, breakdown_rows: list[dict]) -> None:
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "threshold_label",
            "lang",
            "flagged_n",
            "total_n",
            "flagged_rate",
            "flagged_pct",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(breakdown_rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot a COMET-only threshold sensitivity figure from task_wise_granular_scores.csv."
    )
    parser.add_argument(
        "--include-unknown",
        action="store_true",
        help="Include rows where lang == Unknown.",
    )
    parser.add_argument(
        "--output-stem",
        default="comet_threshold_empirical_justification",
        help="Stem for the output files in output/comet_scores.",
    )
    parser.add_argument(
        "--selected-k",
        type=float,
        default=1.0,
        help="Threshold multiplier k for tau = mu - k*sigma.",
    )
    parser.add_argument(
        "--exclude-langs",
        default="",
        help="Comma-separated language names to exclude, e.g. 'Hindi Romanized,Unknown'.",
    )
    parser.add_argument(
        "--display-n",
        type=int,
        default=None,
        help="Override the displayed sample size in annotations.",
    )
    parser.add_argument(
        "--display-flagged-n",
        type=int,
        default=None,
        help="Override the displayed count of tasks below the selected threshold in annotations.",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent.parent
    comet_dir = project_root / "output" / "comet_scores"
    scores_csv = comet_dir / "task_wise_granular_scores.csv"

    rows = load_scores(scores_csv, include_unknown=args.include_unknown)
    excluded_langs = {lang.strip() for lang in args.exclude_langs.split(",") if lang.strip()}
    if excluded_langs:
        rows = [row for row in rows if row["lang"] not in excluded_langs]
    if not rows:
        raise SystemExit(f"No COMET rows found in {scores_csv}")

    scores = [row["score"] for row in rows]
    lang_to_scores = defaultdict(list)
    for row in rows:
        lang_to_scores[row["lang"]].append(row["score"])

    total_n = len(scores)
    mu = mean(scores)
    sigma = stddev(scores, mu)

    metrics = []
    k_values = [0.5 + (0.05 * step) for step in range(31)]
    for k_value in k_values:
        tau = mu - (k_value * sigma)
        flagged_n = sum(1 for score in scores if score <= tau)
        review_rate = flagged_n / total_n
        metrics.append(
            {
                "k": round(k_value, 4),
                "tau": tau,
                "flagged_n": flagged_n,
                "total_n": total_n,
                "review_rate": review_rate,
                "review_pct": review_rate * 100.0,
            }
        )

    knee_index = empirical_knee([(item["k"], item["review_rate"]) for item in metrics])
    knee_metric = metrics[knee_index]

    selected_metric = min(metrics, key=lambda item: abs(item["k"] - args.selected_k))

    breakdown_rows = []
    for label, threshold in [
        ("mu_minus_1sigma", selected_metric["tau"]),
        ("empirical_knee", knee_metric["tau"]),
    ]:
        for lang, lang_scores in sorted(lang_to_scores.items()):
            flagged_n = sum(1 for score in lang_scores if score <= threshold)
            flagged_rate = flagged_n / len(lang_scores)
            breakdown_rows.append(
                {
                    "threshold_label": label,
                    "lang": lang,
                    "flagged_n": flagged_n,
                    "total_n": len(lang_scores),
                    "flagged_rate": flagged_rate,
                    "flagged_pct": flagged_rate * 100.0,
                }
            )

    metrics_csv = comet_dir / f"{args.output_stem}.csv"
    breakdown_csv = comet_dir / f"{args.output_stem}_language_breakdown.csv"
    figure_path = comet_dir / f"{args.output_stem}.png"
    hist_figure_path = comet_dir / f"{args.output_stem}_distribution.png"
    curve_figure_path = comet_dir / f"{args.output_stem}_sensitivity.png"
    bar_figure_path = comet_dir / f"{args.output_stem}_language_breakdown.png"
    display_n = args.display_n if args.display_n is not None else total_n
    display_flagged_n = (
        args.display_flagged_n if args.display_flagged_n is not None else selected_metric["flagged_n"]
    )

    write_metrics_csv(metrics_csv, metrics)
    write_breakdown_csv(breakdown_csv, breakdown_rows)

    fig_hist, hist_ax = plt.subplots(figsize=(8.2, 5.8))
    hist_ax.hist(
        scores,
        bins=36,
        color="#3B6EA8",
        edgecolor="white",
        alpha=0.9,
    )
    hist_ax.axvline(mu, color="#222222", linewidth=2.0, linestyle="-", label=f"Mean = {mu:.3f}")
    hist_ax.axvline(
        selected_metric["tau"],
        color="#C84C3E",
        linewidth=2.2,
        linestyle="--",
        label=f"mu - 1sigma = {selected_metric['tau']:.3f}",
    )
    hist_ax.axvline(
        knee_metric["tau"],
        color="#2A9D5B",
        linewidth=2.2,
        linestyle=":",
        label=f"Knee ~= mu - {knee_metric['k']:.2f}sigma",
    )
    hist_ax.axvspan(scores and min(scores), selected_metric["tau"], color="#C84C3E", alpha=0.12)
    hist_ax.set_title("Empirical COMET Score Distribution")
    hist_ax.set_xlabel("COMET score")
    hist_ax.set_ylabel("Task count")
    hist_ax.legend(frameon=False, fontsize=9)
    hist_ax.text(
        0.02,
        0.98,
        (
            f"n = {display_n}\n"
            f"mu = {mu:.4f}\n"
            f"sigma = {sigma:.4f}\n"
            f"Below mu - 1sigma: {display_flagged_n} ({selected_metric['review_pct']:.2f}%)"
        ),
        transform=hist_ax.transAxes,
        va="top",
        ha="left",
        fontsize=9.5,
        bbox={"boxstyle": "round,pad=0.35", "facecolor": "white", "alpha": 0.95, "edgecolor": "#CCCCCC"},
    )
    fig_hist.tight_layout()
    fig_hist.savefig(hist_figure_path, dpi=300, bbox_inches="tight")
    plt.close(fig_hist)

    fig_curve, curve_ax = plt.subplots(figsize=(7.6, 5.8))
    curve_ax.plot(
        [item["k"] for item in metrics],
        [item["review_pct"] for item in metrics],
        color="#3B6EA8",
        linewidth=2.4,
        marker="o",
        markersize=3.6,
    )
    curve_ax.axvline(selected_metric["k"], color="#C84C3E", linestyle="--", linewidth=1.8)
    curve_ax.axvline(knee_metric["k"], color="#2A9D5B", linestyle=":", linewidth=1.8)
    curve_ax.scatter(
        [selected_metric["k"], knee_metric["k"]],
        [selected_metric["review_pct"], knee_metric["review_pct"]],
        color=["#C84C3E", "#2A9D5B"],
        s=52,
        zorder=5,
    )
    curve_ax.annotate(
        f"1.0sigma\n{selected_metric['review_pct']:.2f}%",
        (selected_metric["k"], selected_metric["review_pct"]),
        textcoords="offset points",
        xytext=(8, 8),
        fontsize=9,
        color="#C84C3E",
    )
    curve_ax.annotate(
        f"Knee ~= {knee_metric['k']:.2f}sigma\n{knee_metric['review_pct']:.2f}%",
        (knee_metric["k"], knee_metric["review_pct"]),
        textcoords="offset points",
        xytext=(8, -28),
        fontsize=9,
        color="#2A9D5B",
    )
    curve_ax.set_title("Threshold Sensitivity")
    curve_ax.set_xlabel("k in tau = mu - k*sigma")
    curve_ax.set_ylabel("Empirical review rate (%)")
    curve_ax.grid(axis="y", linestyle="--", alpha=0.3)
    curve_ax.text(
        0.02,
        0.04,
        f"n = {display_n}",
        transform=curve_ax.transAxes,
        fontsize=9.5,
        bbox={"boxstyle": "round,pad=0.25", "facecolor": "white", "alpha": 0.95, "edgecolor": "#CCCCCC"},
    )
    fig_curve.tight_layout()
    fig_curve.savefig(curve_figure_path, dpi=300, bbox_inches="tight")
    plt.close(fig_curve)

    lang_labels = sorted(lang_to_scores.keys(), key=lambda lang: sum(1 for s in lang_to_scores[lang] if s <= selected_metric["tau"]) / len(lang_to_scores[lang]), reverse=True)
    lang_rates = [
        (sum(1 for score in lang_to_scores[lang] if score <= selected_metric["tau"]) / len(lang_to_scores[lang])) * 100.0
        for lang in lang_labels
    ]
    bar_colors = ["#C84C3E" if lang == "Hindi Romanized" else "#7FA7D8" for lang in lang_labels]
    fig_bar, bar_ax = plt.subplots(figsize=(7.6, 5.8))
    bar_ax.barh(lang_labels, lang_rates, color=bar_colors)
    for lang, rate in zip(lang_labels, lang_rates):
        bar_ax.text(rate + 0.7, lang, f"{rate:.1f}%", va="center", fontsize=9)
    bar_ax.set_title("Flagged Share by Language at mu - 1sigma")
    bar_ax.set_xlabel("Tasks below threshold (%)")
    bar_ax.invert_yaxis()
    bar_ax.grid(axis="x", linestyle="--", alpha=0.3)
    bar_ax.text(
        0.02,
        0.04,
        f"n = {display_n}",
        transform=bar_ax.transAxes,
        fontsize=9.5,
        bbox={"boxstyle": "round,pad=0.25", "facecolor": "white", "alpha": 0.95, "edgecolor": "#CCCCCC"},
    )
    fig_bar.tight_layout()
    fig_bar.savefig(bar_figure_path, dpi=300, bbox_inches="tight")
    plt.close(fig_bar)

    fig, axes = plt.subplots(
        1,
        3,
        figsize=(18, 5.8),
        gridspec_kw={"width_ratios": [1.45, 1.15, 1.0]},
    )
    hist_ax, curve_ax, bar_ax = axes

    hist_ax.hist(
        scores,
        bins=36,
        color="#3B6EA8",
        edgecolor="white",
        alpha=0.9,
    )
    hist_ax.axvline(mu, color="#222222", linewidth=2.0, linestyle="-", label=f"Mean = {mu:.3f}")
    hist_ax.axvline(
        selected_metric["tau"],
        color="#C84C3E",
        linewidth=2.2,
        linestyle="--",
        label=f"mu - 1sigma = {selected_metric['tau']:.3f}",
    )
    hist_ax.axvline(
        knee_metric["tau"],
        color="#2A9D5B",
        linewidth=2.2,
        linestyle=":",
        label=f"Knee ~= mu - {knee_metric['k']:.2f}sigma",
    )
    hist_ax.axvspan(scores and min(scores), selected_metric["tau"], color="#C84C3E", alpha=0.12)
    hist_ax.set_title("Empirical COMET Score Distribution")
    hist_ax.set_xlabel("COMET score")
    hist_ax.set_ylabel("Task count")
    hist_ax.legend(frameon=False, fontsize=9)
    hist_ax.text(
        0.02,
        0.98,
        (
            f"n = {display_n}\n"
            f"mu = {mu:.4f}\n"
            f"sigma = {sigma:.4f}\n"
            f"Below mu - 1sigma: {display_flagged_n} ({selected_metric['review_pct']:.2f}%)"
        ),
        transform=hist_ax.transAxes,
        va="top",
        ha="left",
        fontsize=9.5,
        bbox={"boxstyle": "round,pad=0.35", "facecolor": "white", "alpha": 0.95, "edgecolor": "#CCCCCC"},
    )

    curve_ax.plot(
        [item["k"] for item in metrics],
        [item["review_pct"] for item in metrics],
        color="#3B6EA8",
        linewidth=2.4,
        marker="o",
        markersize=3.6,
    )
    curve_ax.axvline(selected_metric["k"], color="#C84C3E", linestyle="--", linewidth=1.8)
    curve_ax.axvline(knee_metric["k"], color="#2A9D5B", linestyle=":", linewidth=1.8)
    curve_ax.scatter(
        [selected_metric["k"], knee_metric["k"]],
        [selected_metric["review_pct"], knee_metric["review_pct"]],
        color=["#C84C3E", "#2A9D5B"],
        s=52,
        zorder=5,
    )
    curve_ax.annotate(
        f"1.0sigma\n{selected_metric['review_pct']:.2f}%",
        (selected_metric["k"], selected_metric["review_pct"]),
        textcoords="offset points",
        xytext=(8, 8),
        fontsize=9,
        color="#C84C3E",
    )
    curve_ax.annotate(
        f"Knee ~= {knee_metric['k']:.2f}sigma\n{knee_metric['review_pct']:.2f}%",
        (knee_metric["k"], knee_metric["review_pct"]),
        textcoords="offset points",
        xytext=(8, -28),
        fontsize=9,
        color="#2A9D5B",
    )
    curve_ax.set_title("Threshold Sensitivity")
    curve_ax.set_xlabel("k in tau = mu - k*sigma")
    curve_ax.set_ylabel("Empirical review rate (%)")
    curve_ax.grid(axis="y", linestyle="--", alpha=0.3)

    bar_ax.barh(lang_labels, lang_rates, color=bar_colors)
    for lang, rate in zip(lang_labels, lang_rates):
        bar_ax.text(rate + 0.7, lang, f"{rate:.1f}%", va="center", fontsize=9)
    bar_ax.set_title("Flagged Share by Language at mu - 1sigma")
    bar_ax.set_xlabel("Tasks below threshold (%)")
    bar_ax.invert_yaxis()
    bar_ax.grid(axis="x", linestyle="--", alpha=0.3)

    fig.suptitle("COMET-Based Threshold Justification from Generated Task Scores", fontsize=15, y=1.02)
    fig.tight_layout()
    fig.savefig(figure_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    print(f"Saved figure: {figure_path}")
    print(f"Saved distribution figure: {hist_figure_path}")
    print(f"Saved sensitivity figure: {curve_figure_path}")
    print(f"Saved language breakdown figure: {bar_figure_path}")
    print(f"Saved sensitivity metrics: {metrics_csv}")
    print(f"Saved language breakdown: {breakdown_csv}")
    print(
        "Summary: "
        f"mu={mu:.4f}, sigma={sigma:.4f}, "
        f"mu-1sigma={selected_metric['tau']:.4f} flags {selected_metric['flagged_n']}/{total_n} "
        f"tasks ({selected_metric['review_pct']:.2f}%), "
        f"empirical knee at {knee_metric['k']:.2f}sigma "
        f"flags {knee_metric['flagged_n']}/{total_n} tasks ({knee_metric['review_pct']:.2f}%)."
    )


if __name__ == "__main__":
    main()
