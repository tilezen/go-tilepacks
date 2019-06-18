package tilepack

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func NewTapalcatl2JobGenerator(bucket string, pathTemplate string, zooms []uint, materializedZooms []uint) (JobGenerator, error) {
	sess, err := session.NewSession(&aws.Config{})
	if err != nil {
		return nil, err
	}

	downloader := s3manager.NewDownloader(sess)

	return &tapalcatl2JobGenerator{
		s3Client:          downloader,
		bucket:            bucket,
		pathTemplate:      pathTemplate,
		zooms:             zooms,
		materializedZooms: materializedZooms,
	}, nil
}

type tapalcatl2JobGenerator struct {
	s3Client          *s3manager.Downloader
	bucket            string
	pathTemplate      string
	bounds            *LngLatBbox
	zooms             []uint
	materializedZooms []uint
}

func (x *tapalcatl2JobGenerator) CreateWorker() (func(id int, jobs chan *TileRequest, results chan *TileResponse), error) {
	f := func(id int, jobs chan *TileRequest, results chan *TileResponse) {

	}

	return f, nil
}

func (x *tapalcatl2JobGenerator) CreateJobs(jobs chan *TileRequest) error {

}
