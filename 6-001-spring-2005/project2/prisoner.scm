;; 
;;  The play-loop procedure takes as its  arguments two prisoner's
;;  dilemma strategies, and plays an iterated game of approximately
;;  one hundred rounds.  A strategy is a procedure that takes
;;  two arguments: a history of the player's previous plays and 
;;  a history of the other player's previous plays.  The procedure
;;  returns either a "c" for cooperate or a "d" for defect.
;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(define (play-loop strat0 strat1)
  (define (play-loop-iter strat0 strat1 count history0 history1 limit)
    (cond ((= count limit) (print-out-results history0 history1 limit))
	  (else (let ((result0 (strat0 history0 history1))
		      (result1 (strat1 history1 history0)))
		  (play-loop-iter strat0 strat1 (+ count 1)
				  (extend-history result0 history0)
				  (extend-history result1 history1)
				  limit)))))
  (play-loop-iter strat0 strat1 0 the-empty-history the-empty-history
		  (+ 90 (random 21))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;;  The following procedures are used to compute and print
;;  out the players' scores at the end of an iterated game
;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (print-out-results history0 history1 number-of-games)
  (let ((scores (get-scores history0 history1)))
    (newline)
    (display "Player 1 Score:  ")
    (display (* 1.0 (/ (car scores) number-of-games)))
    (newline)
    (display "Player 2 Score:  ")
    (display (* 1.0 (/ (cadr scores) number-of-games)))
    (newline)))

(define (get-scores history0 history1)
  (define (get-scores-helper history0 history1 score0 score1)
    (cond ((empty-history? history0)
	   (list score0 score1))
	  (else (let ((game (make-play (most-recent-play history0)
				       (most-recent-play history1))))
		  (get-scores-helper (rest-of-plays history0)
				     (rest-of-plays history1)
				     (+ (get-player-points 0 game) score0)
				     (+ (get-player-points 1 game) score1))))))
  (get-scores-helper history0 history1 0 0))

(define (get-player-points num game)
  (list-ref (get-point-list game) num))

(define *game-association-list*
  ;; format is that first sublist identifies the players' choices 
  ;; with "c" for cooperate and "d" for defect; and that second sublist 
  ;; specifies payout for each player
  '((("c" "c") (3 3))
    (("c" "d") (0 5))
    (("d" "c") (5 0))
    (("d" "d") (1 1))))

(define (get-point-list game)
  (cadr (extract-entry game *game-association-list*)))

;; note that you will need to write extract-entry

(define (extract-entry key alist)
		(cond ((null? alist) #f)
			  ((equal? key (caar alist)) (list key (cadar alist)))
			  (else (extract-entry key (cdr alist)))))

(define make-play list)

(define the-empty-history '())

(define extend-history cons)
(define empty-history? null?)

(define most-recent-play car)
(define rest-of-plays cdr)

;; A sampler of strategies

(define (NASTY my-history other-history)
  "d")

(define (PATSY my-history other-history)
  "c")

(define (SPASTIC my-history other-history)
  (if (= (random 2) 0)
      "c"
      "d"))

(define (EGALITARIAN  my-history other-history)
  (define (count-instances-of test hist)
    (cond ((empty-history? hist) 0)
	  ((string=? (most-recent-play hist) test)
	   (+ (count-instances-of test (rest-of-plays hist)) 1))
	  (else (count-instances-of test (rest-of-plays hist)))))
  (let ((ds (count-instances-of "d" other-history))
	(cs (count-instances-of "c" other-history)))
    (if (> ds cs) "d" "c")))

;; It's because the time for EGALITARIAN to make a choice is theta(n)
;; The order of growth for the two procedures is same. The newer version will
;; be faster, since it's iterative.

(define (EYE-FOR-EYE my-history other-history)
  (if (empty-history? my-history)
      "c"
      (most-recent-play other-history)))


(define all-strats
	(list (list "NASTY" NASTY) 
		  (list "PATSY" PATSY)
		  (list "SPASTIC" SPASTIC) 
		  (list "EGALITARIAN" EGALITARIAN) 
		  (list "EYE-FOR-EYE" EYE-FOR-EYE)))

(define (play-with-pairs lstrats rstrats)
	(define (make-pairs llist rlist)
			(if (null? rlist)
				(list)
				(append (make-pairs llist (cdr rlist))
						(map (lambda (l) (list l (car rlist)))
							 llist))))
	(for-each (lambda (p) 
					  (display (caar p))
					  (display " vs ")
					  (display (caadr p))
					  (play-loop (cadar p) (cadadr p))
					  (newline))
		 (make-pairs lstrats rstrats)))

(play-with-pairs all-strats all-strats)

(define (EYE-FOR-TWO-EYES my-history other-history)
		(if (and (>= (length other-history) 2)
				 (string=? (most-recent-play other-history) "d")
				 (string=? (most-recent-play (rest-of-plays other-history)) "d"))
			"d"
			"c"))

(play-with-pairs all-strats (list (list "EYE-FOR-TWO-EYES" EYE-FOR-TWO-EYES)))

(define (make-eye-for-n-eyes n)
		(define (is-defect? history times)
				(cond ((= times 0) #t)
					  ((< (length history) times) #f)
					  (else (and (is-defect? (rest-of-plays history) (- times 1))
						 		 (string=? (most-recent-play history) "d")))))
		(lambda (my-history other-history)
				(if (is-defect? other-history n)
					"d"
					"c")))


(play-with-pairs all-strats (list (list "EYE-FOR-N-EYES (n = 2)" (make-eye-for-n-eyes 2))))

(define (make-rotating-strategy strat0 strat1 freq0 freq1)
		(lambda (my-history other-history)
				(if (< (remainder (length my-history) (+ freq0 freq1)) freq0)
					(strat0 my-history other-history)
					(strat1 my-history other-history))))

(play-with-pairs all-strats (list (list "ROTATING-STRATEGY" (make-rotating-strategy NASTY PATSY 1 1))))

(define (make-higher-order-spastic strategies)
		(define (nth-element items n)
				(if (= n 1) 
					(car items)
					(nth-element (cdr items) (- n 1))))
		(lambda (my-history other-history)
				(let ((n (+ 1 (remainder (length my-history) (length strategies)))))
					 ((nth-element strategies n) my-history other-history))))

(play-with-pairs all-strats (list (list "HIGHER-ORDER-SPASTIC" (make-higher-order-spastic (list NASTY PATSY SPASTIC)))))

(define (gentle strat gentleness-factor)
		(lambda (my-history other-history)
				(let ((choice (strat my-history other-history)))
					 (if (and (string=? choice "d") (> (random 1.0) gentleness-factor))
					 	 "d"
					 	 "c"))))

(define SLIGHTLY-GENTLE-NASTY (gentle NASTY 0.1))

(play-with-pairs all-strats (list (list "SLIGHTLY-GENTLE-NASTY" SLIGHTLY-GENTLE-NASTY)))

(define SLIGHTLY-GENTLE-EYE-FOR-EYE (gentle EYE-FOR-EYE 0.1))

(play-with-pairs all-strats (list (list "SLIGHTLY-GENTLE-EYE-FOR-EYE" SLIGHTLY-GENTLE-EYE-FOR-EYE)))

;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; code to use in 3 player game
;;

(define *game-association-list*
  (list (list (list "c" "c" "c") (list 4 4 4))
        (list (list "c" "c" "d") (list 2 2 5))
        (list (list "c" "d" "c") (list 2 5 2))
        (list (list "d" "c" "c") (list 5 2 2))
        (list (list "c" "d" "d") (list 0 3 3))
        (list (list "d" "c" "d") (list 3 0 3))
        (list (list "d" "d" "c") (list 3 3 0))
        (list (list "d" "d" "d") (list 1 1 1))))

(define (play-loop-3 strat0 strat1 strat2)
  (define (play-loop-iter-3 strat0 strat1 strat2 count history0 history1 history2 limit)
    (cond ((= count limit) (print-out-results-3 history0 history1 history2 limit))
	  (else (let ((result0 (strat0 history0 history1 history2))
		      	  (result1 (strat1 history1 history0 history2))
		      	  (result2 (strat2 history2 history0 history1)))
		  (play-loop-iter-3 strat0 strat1 strat2 (+ count 1)
				  (extend-history result0 history0)
				  (extend-history result1 history1)
				  (extend-history result2 history2)
				  limit)))))
  (play-loop-iter-3 strat0 strat1 strat2 0 the-empty-history the-empty-history the-empty-history
		  (+ 90 (random 21))))

(define (print-out-results-3 history0 history1 history2 number-of-games)
  (let ((scores (get-scores-3 history0 history1 history2)))
    (newline)
    (display "Player 1 Score:  ")
    (display (* 1.0 (/ (car scores) number-of-games)))
    (newline)
    (display "Player 2 Score:  ")
    (display (* 1.0 (/ (cadr scores) number-of-games)))
    (newline)
    (display "Player 3 Score:  ")
    (display (* 1.0 (/ (caddr scores) number-of-games)))
    (newline)))

(define (get-scores-3 history0 history1 history2)
  (define (get-scores-helper-3 history0 history1 history2 score0 score1 score2)
    (cond ((empty-history? history0)
	   (list score0 score1 score2))
	  (else (let ((game (make-play (most-recent-play history0)
				       (most-recent-play history1)
				       (most-recent-play history2))))
		  (get-scores-helper-3 (rest-of-plays history0)
				     (rest-of-plays history1)
				     (rest-of-plays history2)
				     (+ (get-player-points 0 game) score0)
				     (+ (get-player-points 1 game) score1)
				     (+ (get-player-points 2 game) score2))))))
  (get-scores-helper-3 history0 history1 history2 0 0 0))

(define (NASTY-3 my-history other-history0 other-history1)
		"d")

(define (PATSY-3 my-history other-history0 other-history1)
		"c")

(define (SPASTIC-3 my-history other-history0 other-history1)
  		(if (= (random 2) 0)
      		"c"
      		"d"))

(play-loop-3 PATSY-3 PATSY-3 PATSY-3)

(define (TOUGH-EYE-FOR-EYE-3 my-history other-history0 other-history1)
		(cond ((empty-history? my-history) "c")
			  ((or (string=? (most-recent-play other-history0) "d") (string=? (most-recent-play other-history1) "d")) "d")
			  (else "c")))

(define (SOFT-EYE-FOR-EYE-3 my-history other-history0 other-history1)
		(cond ((empty-history? my-history) "c")
			  ((and (string=? (most-recent-play other-history0) "d") (string=? (most-recent-play other-history1) "d")) "d")
			  (else "c")))

(play-loop-3 SPASTIC-3 TOUGH-EYE-FOR-EYE-3 SOFT-EYE-FOR-EYE-3)

(play-loop-3 NASTY-3 TOUGH-EYE-FOR-EYE-3 SOFT-EYE-FOR-EYE-3)

(define (make-combined-strategies strat0 strat1 combine)
		(lambda (my-history other-history0 other-history1)
				(combine (strat0 my-history other-history0)
				 		 (strat1 my-history other-history1))))
 
(play-loop-3 SPASTIC-3 (make-combined-strategies Eye-for-Eye Eye-for-Eye (lambda (r1 r2) (if (or (string=? r1 "d") (string=? r2 "d")) "d" "c"))) SOFT-EYE-FOR-EYE-3)

(define (make-history-summary hist0 hist1 hist2)
		(define empty-summary (list (list 0 0 0) (list 0 0 0) (list 0 0 0)))
		(define (summary-inc! summary x y) (list-set! (list-ref summary x) y (+ (list-ref (list-ref summary x) y) 1)))
		(define (get-x r1 r2) 
				(cond ((and (string=? r1 "c") (string=? r2 "c")) 0) 
					  ((and (string=? r1 "d") (string=? r2 "d")) 2)
					  (else 1)))
		(define (get-y r0)
				(if (string=? r0 "c")
					0
					1))
		(if (< (length hist0) 2)
			empty-summary
			(let ((summary (make-history-summary (cdr hist0) (cdr hist1) (cdr hist2)))
				  (r0 (car hist0))
				  (r1 (cadr hist1))
				  (r2 (cadr hist2)))
				 (summary-inc! summary (get-x r1 r2) (get-y r0))
				 (summary-inc! summary (get-x r1 r2) 2)
				 summary)))

(define summary 
		(make-history-summary (list "c" "c" "d" "d")
							  (list "c" "d" "d" "d")
							  (list "c" "c" "c" "d")))


(define (get-probability-of-c sum)
		(if (null? sum)
			'()
			(let ((cnt (car sum)))
				 (if (= (list-ref cnt 2) 0)
				 	 (cons '() (get-probability-of-c (cdr sum)))
				 	 (cons (/ (list-ref cnt 0) (list-ref cnt 2)) (get-probability-of-c (cdr sum)))))))


;; in expected-values: #f = don't care 
;;                      X = actual-value needs to be #f or X 
(define (test-entry expected-values actual-values) 
   (cond ((null? expected-values) (null? actual-values)) 
         ((null? actual-values) #f) 
         ((or (not (car expected-values)) 
              (not (car actual-values)) 
              (= (car expected-values) (car actual-values))) 
          (test-entry (cdr expected-values) (cdr actual-values))) 
         (else #f))) 

(define (is-he-a-fool? hist0 hist1 hist2) 
   (test-entry (list 1 1 1) 
               (get-probability-of-c 
                (make-history-summary hist0 hist1 hist2))))

(define (could-he-be-a-fool? hist0 hist1 hist2)
  (test-entry (list 1 1 1)
              (map (lambda (elt) 
                      (cond ((null? elt) 1)
                            ((= elt 1) 1)  
                            (else 0)))
                   (get-probability-of-c (make-history-summary hist0 
                                                               hist1
                                                               hist2)))))


(define (is-he-a-soft? hist0 hist1 hist2)
		(test-entry (list 1 1 0)
					(get-probability-of-c (make-history-summary hist0 
																hist1 
																hist2))))

(define (DONT-TOLERATE-FOOLS my-history other-history0 other-history1)
		(cond ((< (length my-history) 10) "c")
			  ((and (could-he-be-a-fool? other-history0 my-history other-history1)
			  		(could-he-be-a-fool? other-history1 my-history other-history0)) "d")
			  (else "c")))

(play-loop-3 PATSY-3 PATSY-3 DONT-TOLERATE-FOOLS)

