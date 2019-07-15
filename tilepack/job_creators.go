package tilepack

type JobGenerator interface {
	CreateWorker() (func(id int, jobs chan *TileRequest, results chan *TileResponse), error)
	CreateJobs(jobs chan *TileRequest) error
}
