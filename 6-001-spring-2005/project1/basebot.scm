;;; Project 1, 6.001, Spring 2005

;;; idea is to simulate a baseball robot

;; imagine hitting a ball with an initial velocity of v 
;; at an angle alpha from the horizontal, at a height h
;; we would like to know how far the ball travels.

;; as a first step, we can just model this with simple physics
;; so the equations of motion for the ball have a vertical and a 
;; horizontal component

;; the vertical component is governed by
;; y(t) = v sin alpha t + h - 1/2 g t^2 
;; where g is the gravitational constant of 9.8 m/s^2

;; the horizontal component is governed by
;; x(t) = v cos alpha t
;; assuming it starts at the origin

;; First, we want to know when the ball hits the ground
;; this is governed by the quadratic equation, so we just need to know when 
;; y(t)=0 (i.e. for what t_impact is y(t_impact)= 0).
;; note that there are two solutions, only one makes sense physically

(define square
  (lambda (x) (* x x)))

;; these are constants that will be useful to us
(define gravity 9.8)  ;; in m/s
(define pi 3.14159)

;; Problem 1

(define position
  (lambda (a v u t)
    (+ (* 0.5 a t t) (* v t) u)))

;; you need to complete this procedure, then show some test cases

(position 0 0 0 0) ; -> 0
(position 0 0 20 0) ; -> 20
(position 0 5 10 10) ; -> 60
(position 2 2 2 2) ; -> 10
(position 5 5 5 5) ; -> 92.5


;; Problem 2

(define root1
  (lambda (a b c)
    (/ (- (- b) (sqrt (- (* b b) (* 4 a c)))) (* 2 a))))

(root1 1 -3 2) ; -> 1

(define root2
  (lambda (a b c)
    (/ (+ (- b) (sqrt (- (* b b) (* 4 a c)))) (* 2 a))))

(root2 1 -3 2) ; -> 2

;; complete these procedures and show some test cases

;; Problem 3

(define time-to-impact
  (lambda (vertical-velocity elevation)
    (root1 (- 4.9) vertical-velocity elevation)))


(time-to-impact 10 10) ; ->  2.77598
(time-to-impact 0 10) ; -> 1.42857

;; Note that if we want to know when the ball drops to a particular height r 
;; (for receiver), we have

(define time-to-height
  (lambda (vertical-velocity elevation target-elevation)
    (time-to-impact vertical-velocity (- elevation target-elevation))))

(time-to-height 10 10 0) ; -> 2.77598

;; Problem 4

;; once we can solve for t_impact, we can use it to figure out how far the ball went

;; conversion procedure

(define pi (acos -1))

(define degree2radian
  (lambda (deg)
    (/ (*  deg pi) 180.)))

(define travel-distance-simple
  (lambda (elevation velocity angle)
    (* (* velocity (cos (degree2radian angle))) 
       (time-to-impact (* velocity (sin (degree2radian angle))) elevation))))

;; let's try this out for some example values.  Note that we are going to 
;; do everything in metric units, but for quaint reasons it is easier to think
;; about things in English units, so we will need some conversions.

(define meters-to-feet
  (lambda (m)
    (/ (* m 39.6) 12)))

(define feet-to-meters
  (lambda (f)
    (/ (* f 12) 39.6)))

(define hours-to-seconds
  (lambda (h)
    (* h 3600)))

(define seconds-to-hours
  (lambda (s)
    (/ s 3600)))

;; what is time to impact for a ball hit at a height of 1 meter
;; with a velocity of 45 m/s (which is about 100 miles/hour)
;; at an angle of 0 (straight horizontal)
;; at an angle of (/ pi 2) radians or 90 degrees (straight vertical)
;; at an angle of (/ pi 4) radians or 45 degrees

;; what is the distance traveled in each case?
;; record both in meters and in feet

(travel-distance-simple 1 45 0) ; -> 20.3289
(meters-to-feet (travel-distance-simple 1 45 0)) ; -> 67.0854
(travel-distance-simple 1 45 90) ; -> 0.0000
(meters-to-feet (travel-distance-simple 1 45 90)) ; 0.0000
(travel-distance-simple 1 45 45) ; -> 207.6279
(meters-to-feet (travel-distance-simple 1 45 45)) ; -> 685.1719


;; Problem 5

;; these sound pretty impressive, but we need to look at it more carefully

;; first, though, suppose we want to find the angle that gives the best
;; distance
;; assume that angle is between 0 and (/ pi 2) radians or between 0 and 90
;; degrees

(define alpha-increment 0.01)

(define find-best-angle
  (lambda (velocity elevation)
  	(define (try-it angle best-angle longest-distance)
  			(if (> angle 90) 
  				best-angle
  				(if (> (travel-distance-simple elevation velocity angle) longest-distance)
  					(try-it (+ angle alpha-increment) angle (travel-distance-simple elevation velocity angle))
  					(try-it (+ angle alpha-increment) best-angle longest-distance))))
  	(try-it 0.00 90 0.00)))

;; find best angle
;; try for other velocities
;; try for other heights

(find-best-angle 10 10) ; -> 30.7

;; Problem 6

;; problem is that we are not accounting for drag on the ball (or on spin 
;; or other effects, but let's just stick with drag)
;;
;; Newton's equations basically say that ma = F, and here F is really two 
;; forces.  One is the effect of gravity, which is captured by mg.  The
;; second is due to drag, so we really have
;;
;; a = drag/m + gravity
;;
;; drag is captured by 1/2 C rho A vel^2, where
;; C is the drag coefficient (which is about 0.5 for baseball sized spheres)
;; rho is the density of air (which is about 1.25 kg/m^3 at sea level 
;; with moderate humidity, but is about 1.06 in Denver)
;; A is the surface area of the cross section of object, which is pi D^2/4 
;; where D is the diameter of the ball (which is about 0.074m for a baseball)
;; thus drag varies by the square of the velocity, with a scaling factor 
;; that can be computed

;; We would like to again compute distance , but taking into account 
;; drag.
;; Basically we can rework the equations to get four coupled linear 
;; differential equations
;; let u be the x component of velocity, and v be the y component of velocity
;; let x and y denote the two components of position (we are ignoring the 
;; third dimension and are assuming no spin so that a ball travels in a plane)
;; the equations are
;;
;; dx/dt = u
;; dy/dt = v
;; du/dt = -(drag_x/m + g_x)
;; dv/dt = -(drag_y/m + g_y)
;; we have g_x = - and g_y = - gravity
;; to get the components of the drag force, we need some trig.
;; let speeed = (u^2+v^2)^(1/2), then
;; drag_x = - drag * u /speed
;; drag_y = - drag * v /speed
;; where drag = beta speed^2
;; and beta = 1/2 C rho pi D^2/4
;; note that we are taking direction into account here

;; we need the mass of a baseball -- which is about .15 kg.

;; so now we just need to write a procedure that performs a simple integration
;; of these equations -- there are more sophisticated methods but a simple one 
;; is just to step along by some step size in t and add up the values

;; dx = u dt
;; dy = v dt
;; du = - 1/m speed beta u dt
;; dv = - (1/m speed beta v + g) dt

;; initial conditions
;; u_0 = V cos alpha
;; v_0 = V sin alpha
;; y_0 = h
;; x_0 = 0

;; we want to start with these initial conditions, then take a step of size dt
;; (which could be say 0.1) and compute new values for each of these parameters
;; when y reaches the desired point (<= 0) we stop, and return the distance (x)

(define drag-coeff 0.5)
(define density 1.25)  ; kg/m^3
(define mass .145)  ; kg
(define diameter 0.074)  ; m
(define beta (* .5 drag-coeff density (* 3.14159 .25 (square diameter))))

(define integrate
  (lambda (x0 y0 u0 v0 dt g m beta)
  	(define speed (sqrt (+ (* u0 u0) (* v0 v0))))
    (if (>= y0 0) 
    	(integrate (+ x0 (* u0 dt)) 
    			   (+ y0 (* v0 dt)) 
    			   (- u0 (/ (* speed beta u0 dt) m)) 
    			   (- v0 (* (+ (/ (* speed beta v0) m) g) dt)) 
    			   dt
    			   g
    			   m
    			   beta)
    	x0))))

(define travel-distance
  (lambda (elevation velocity angle)
  			(integrate 0 
  					   elevation 
  					   (* velocity (cos (degree2radian angle))) 
  					   (* velocity (sin (degree2radian angle))) 
  					   0.01
  					   9.8
  					   mass
  					   beta)))

;; RUN SOME TEST CASES

(travel-distance 1 45 0) ; -> 19.3425
(travel-distance 1 45 90) ; -> 0.0000
(travel-distance 1 45 45) ; -> 92.2305

;; what about Denver?

;; Problem 7
 
;; now let's turn this around.  Suppose we want to throw the ball.  The same
;; equations basically hold, except now we would like to know what angle to 
;; use, given a velocity, in order to reach a given height (receiver) at a 
;; given distance


;; a cather trying to throw someone out at second has to get it roughly 36 m
;; (or 120 ft) how quickly does the ball get there, if he throws at 55m/s,
;;  at 45m/s, at 35m/s?

;; try out some times for distances (30, 60, 90 m) or (100, 200, 300 ft) 
;; using 45m/s

(define (make-result di ti) (cons di ti))

(define (dis p) (car p))

(define (tim p) (cdr p))

(define integrate2
  (lambda (x0 y0 u0 v0 dt ti g m beta)
  	(define speed (sqrt (+ (* u0 u0) (* v0 v0))))
    (if (>= y0 0) 
    	(integrate2 (+ x0 (* u0 dt)) 
	    		(+ y0 (* v0 dt)) 
   			    (- u0 (/ (* speed beta u0 dt) m)) 
   			    (- v0 (* (+ (/ (* speed beta v0) m) g) dt)) 
   			    dt
   			    (+ ti dt)
   			    g
   			    m
   			    beta)
   	(make-result x0 ti)))))

(define travel-distance2
  (lambda (elevation velocity angle)
  			(integrate2 0 
  					    elevation 
  					    (* velocity (cos (degree2radian angle))) 
  					    (* velocity (sin (degree2radian angle))) 
  					    0.01
  					    0
  					    9.8
  					    mass
  					    beta)))

(define (close-enug actual expect)
		(< (abs (- actual expect)) 0.01))

(define (find-shortest-time elevation velocity distance)
		(define (try-it angle best-angle shortest-time)
				(newline)
				(display best-angle)
				(if (> angle 90)
					shortest-time
					(if (and (close-enug (dis (travel-distance2 elevation velocity angle)) distance)
							(or (= best-angle -91)
							 	(> shortest-time (tim (travel-distance2 elevation velocity angle)))))
						(try-it (+ angle alpha-increment) angle (tim (travel-distance2 elevation velocity angle)))
						(try-it (+ angle alpha-increment) best-angle shortest-time))))
		(try-it -90 -91 0))

(find-shortest-time 1 45 19.3425) ; -> 0.1
(find-shortest-time 1 45 0) ; -> 0.1


;; Problem 8

(define (travel-distance3 elevation velocity angle num-bounces)
  		(define result
  			(integrate 0
  					   elevation 
  					   (* velocity (cos (degree2radian angle))) 
  					   (* velocity (sin (degree2radian angle))) 
  					   0.01
  					   9.8
  					   mass
  					   beta))
  		(if (= num-bounces 0)
  			result
  			(+ result (travel-distance3 0 (* 0.5 velocity) angle (- num-bounces 1)))))

(travel-distance3 1 45 45 1) ; -> 130.6107

;; Problem 9

(define (make-result di ve) (cons di ve))

(define (dis p) (car p))

(define (vel p) (cdr p))

(define integrate4
  (lambda (x0 y0 u0 v0 dt g m beta)
  	(define speed (sqrt (+ (* u0 u0) (* v0 v0))))
    (if (>= y0 0) 
    	(integrate4 (+ x0 (* u0 dt)) 
    			   (+ y0 (* v0 dt)) 
    			   (- u0 (/ (* speed beta u0 dt) m)) 
    			   (- v0 (* (+ (/ (* speed beta v0) m) g) dt)) 
    			   dt
    			   g
    			   m
    			   beta)
    	(make-result x0 speed))))


(define (travel-distance4 elevation velocity angle num-bounces)
  		(define result
  			(integrate4 0
  					    elevation 
  					    (* velocity (cos (degree2radian angle))) 
  					    (* velocity (sin (degree2radian angle))) 
  					    0.01
  					    9.8
  					    mass
  					    beta))
  		(if (= num-bounces 0)
  			(dis result)
  			(+ (dis result) (travel-distance4 0 (* 0.5 (vel result)) angle (- num-bounces 1)))))

(travel-distance4 1 45 45 1) ; -> 105.3791
