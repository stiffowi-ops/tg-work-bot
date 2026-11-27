# WorkHub — Go bootstrap

This branch is a separate test service; the existing Python Telegram bot on main is unchanged.

Host settings: branch workspace-go-v1, Dockerfile at repository root, Docker build context '.', port 8080, use the image ENTRYPOINT (no manual command override). The Go source entry point is main.go.

Check GET / and GET /api/health (HTTP 200, ok: true). Local: go test ./...; go run .; Docker: docker build -t workhub . && docker run --rm -p 8080:8080 workhub.

This initial service does not start Telegram polling, does not include real internal content, and does not implement authentication. Do not route production traffic to it or configure the production bot token. Use an isolated test service. Next: component-based frontend with demo data, then verified Telegram initData authorization and test bot integration.
