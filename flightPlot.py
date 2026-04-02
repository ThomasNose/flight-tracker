import mplcursors


def update(fig, ax, flight_data):
    ax.cla()

    lons = [r[5] for r in flight_data]
    lats = [r[6] for r in flight_data]

    scatter = ax.scatter(lons, lats, s=1, color="cyan", alpha=0.6)
    ax.set_xlim(-180, 180)
    ax.set_ylim(-90, 90)
    ax.set_facecolor("#0f0f1a")
    ax.set_title(f"Live flights — {len(lons)} aircraft", color="white")
    fig.patch.set_facecolor("#0f0f1a")

    cursor = mplcursors.cursor(scatter, hover=True)

    @cursor.connect("add")
    def on_add(sel):
        r = flight_data[sel.index]
        sel.annotation.set_text(
            f"Callsign:  {r[1] or 'N/A'}\n"
            f"Country:   {r[2]}\n"
            f"Altitude:  {r[7]}m\n"
            f"Velocity:  {r[9]} m/s\n"
            f"Track:     {r[10]}°\n"
            f"Squawk:    {r[14]}"
        )
        sel.annotation.get_bbox_patch().set(fc="#1a1a2e", alpha=0.9)
        sel.annotation.set_color("white")