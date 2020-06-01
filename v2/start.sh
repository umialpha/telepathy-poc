go run worker/worker.go &>wk.log &
go run backend/backend.go &>be.log &
go run frontend/frontend.go &>fe.log &
go run client/client.go -JOB_ID job-test-12
