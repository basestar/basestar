#Test containers

For running docker containers in tests, for a given spec, only one container instance
 will be run. Instance will NOT be terminated at end of tests, rather it will be re-used
 for another run. This makes tests faster, and local development a lot smoother.

 Because instances are re-used, it is important to make sure that different resources
 are used within each container, e.g. always create queues/tables/etc with new random names.
