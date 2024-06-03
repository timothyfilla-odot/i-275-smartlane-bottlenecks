1. This script is able to be run as part of a weekly ETL or manually to populate bottleneck data for I-275 SmartLane (Phase 1 WB, going live in Spring 2024.)
2. In main.py, use either line 11 or line 12, not both. Line 11 should be used for an ETL; line 12 for a manual run.
3. Run main.py. This will call the INRIX Bottlenecks API; process the bottleneck files; and write the bottlenecks file to the warehouse in three tables used to power the I-275 dashboard.
4. Files downloaded from the Inrix API move in this order: Zip -> processed_zips / ready_for_warehouse -> processed_csvs.
5. This script will need to be adjusted to run for Phase 2 of the I-275 Smartlane, which is not scheduled to go live until ~2028.