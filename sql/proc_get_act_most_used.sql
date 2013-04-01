DELIMITER //
DROP PROCEDURE IF EXISTS get_act_most_used//
CREATE PROCEDURE get_act_most_used()
BEGIN
	DECLARE a CHAR(80);
	DECLARE done INT DEFAULT FALSE;
	DECLARE cur1 CURSOR FOR SELECT name FROM Resources;
	
	OPEN cur1;
	
	read_loop: LOOP
		FETCH cur1 into a;
		IF done THEN
			LEAVE read_loop;
		END IF;
	END LOOP;
END;
