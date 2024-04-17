SELECT
    tm.*,
    f.status,
	f.alive,
	f.date
FROM
    trademark as tm
LEFT JOIN
    (
		SELECT DISTINCT ON (trademark_serial)
		trademark_serial, status, alive, date
		FROM filing
		ORDER BY trademark_serial, date DESC
	) f
ON tm.serial_number = f.trademark_serial
WHERE f.date IN ({{dates|datelist}})
LIMIT 10
