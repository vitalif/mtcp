#define _LARGEFILE64_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <dirent.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <signal.h>
#include <limits.h>

#include <mtcp_api.h>
#include <mtcp_epoll.h>

#include "cpu.h"
#include "netlib.h"
#include "debug.h"

#define MAX_FLOW_NUM  (10000)

#define RCVBUF_SIZE 9000

#define MAX_EVENTS (MAX_FLOW_NUM * 3)

#ifndef TRUE
#define TRUE (1)
#endif

#ifndef FALSE
#define FALSE (0)
#endif

#ifndef ERROR
#define ERROR (-1)
#endif

#define HT_SUPPORT FALSE

#ifndef MAX_CPUS
#define MAX_CPUS		16
#endif
/*----------------------------------------------------------------------------*/
struct server_vars
{
	int recv_len;
	int request_len;
	long int total_read, total_sent;
	uint8_t done;
	uint8_t rspheader_sent;
	uint8_t keep_alive;
};
/*----------------------------------------------------------------------------*/
struct thread_context
{
	mctx_t mctx;
	int ep;
	struct server_vars *svars;
};
/*----------------------------------------------------------------------------*/
static int num_cores;
static int core_limit;
static pthread_t app_thread[MAX_CPUS] = { 0 };
static int done[MAX_CPUS] = { 0 };
static char *conf_file = NULL;
static int backlog = -1;
static char *peer_addr = NULL;
static int peer_port = 7777;
static int ping_size = 4096;
/*----------------------------------------------------------------------------*/
void CleanServerVariable(struct server_vars *sv)
{
	sv->recv_len = 0;
	sv->request_len = 0;
	sv->total_read = 0;
	sv->total_sent = 0;
	sv->done = 0;
	sv->rspheader_sent = 0;
	sv->keep_alive = 0;
}
/*----------------------------------------------------------------------------*/
void CloseConnection(struct thread_context *ctx, int sockid, struct server_vars *sv)
{
	mtcp_epoll_ctl(ctx->mctx, ctx->ep, MTCP_EPOLL_CTL_DEL, sockid, NULL);
	mtcp_close(ctx->mctx, sockid);
}
/*----------------------------------------------------------------------------*/
static int read_poll(struct thread_context *ctx, int sockid, void *buf, int len)
{
	while (len > 0)
	{
		int rcv = mtcp_read(ctx->mctx, sockid, buf, len);
		if (rcv < 0)
		{
			if (errno == EAGAIN)
				continue;
			printf("read() failed: %d (%s).\n", errno, strerror(errno));
			return -errno;
		}
		len -= rcv;
		buf += rcv;
	}
	return 0;
}
/*----------------------------------------------------------------------------*/
static int write_poll(struct thread_context *ctx, int sockid, void *buf, int len)
{
	while (len > 0)
	{
		int sent = mtcp_write(ctx->mctx, sockid, buf, len);
		if (sent < 0)
		{
			if (errno == EAGAIN)
				continue;
			printf("write() failed: %d (%s).\n", errno, strerror(errno));
			return -errno;
		}
		len -= sent;
		buf += sent;
	}
	return 0;
}
/*----------------------------------------------------------------------------*/
static int HandleReadEvent(struct thread_context *ctx, int sockid, struct server_vars *sv)
{
	char buf[RCVBUF_SIZE];
	int rd;

	rd = mtcp_read(ctx->mctx, sockid, buf, RCVBUF_SIZE);
	if (rd <= 0)
	{
		return rd;
	}

	return write_poll(ctx, sockid, buf, rd);
}
/*----------------------------------------------------------------------------*/
int AcceptConnection(struct thread_context *ctx, int listener)
{
	mctx_t mctx = ctx->mctx;
	struct server_vars *sv;
	struct mtcp_epoll_event ev;
	int c;

	c = mtcp_accept(mctx, listener, NULL, NULL);

	if (c >= 0)
	{
		if (c >= MAX_FLOW_NUM)
		{
			TRACE_ERROR("Invalid socket id %d.\n", c);
			return -1;
		}

		sv = &ctx->svars[c];
		CleanServerVariable(sv);
		TRACE_APP("New connection %d accepted.\n", c);
		ev.events = MTCP_EPOLLIN;
		ev.data.sockid = c;
		mtcp_setsock_nonblock(ctx->mctx, c);
		mtcp_epoll_ctl(mctx, ctx->ep, MTCP_EPOLL_CTL_ADD, c, &ev);
		TRACE_APP("Socket %d registered.\n", c);
	}
	else
	{
		if (errno != EAGAIN)
		{
			TRACE_ERROR("mtcp_accept() error %s\n", strerror(errno));
		}
	}

	return c;
}
/*----------------------------------------------------------------------------*/
struct thread_context *InitializeServerThread(int core)
{
	struct thread_context *ctx;

	/* affinitize application thread to a CPU core */
#if HT_SUPPORT
	mtcp_core_affinitize(core + (num_cores / 2));
#else
	mtcp_core_affinitize(core);
#endif /* HT_SUPPORT */

	ctx = (struct thread_context *)calloc(1, sizeof(struct thread_context));
	if (!ctx)
	{
		TRACE_ERROR("Failed to create thread context!\n");
		return NULL;
	}

	/* create mtcp context: this will spawn an mtcp thread */
	ctx->mctx = mtcp_create_context(core);
	if (!ctx->mctx)
	{
		TRACE_ERROR("Failed to create mtcp context!\n");
		free(ctx);
		return NULL;
	}

	/* create epoll descriptor */
	ctx->ep = mtcp_epoll_create(ctx->mctx, MAX_EVENTS);
	if (ctx->ep < 0)
	{
		mtcp_destroy_context(ctx->mctx);
		free(ctx);
		TRACE_ERROR("Failed to create epoll descriptor!\n");
		return NULL;
	}

	/* allocate memory for server variables */
	ctx->svars = (struct server_vars *)calloc(MAX_FLOW_NUM, sizeof(struct server_vars));
	if (!ctx->svars)
	{
		mtcp_close(ctx->mctx, ctx->ep);
		mtcp_destroy_context(ctx->mctx);
		free(ctx);
		TRACE_ERROR("Failed to create server_vars struct!\n");
		return NULL;
	}

	return ctx;
}
/*----------------------------------------------------------------------------*/
int CreateListeningSocket(struct thread_context *ctx)
{
	int listener;
	struct mtcp_epoll_event ev;
	struct sockaddr_in saddr;
	int ret;

	/* create socket and set it as nonblocking */
	listener = mtcp_socket(ctx->mctx, AF_INET, SOCK_STREAM, 0);
	if (listener < 0)
	{
		TRACE_ERROR("Failed to create listening socket!\n");
		return -1;
	}

	ret = mtcp_setsock_nonblock(ctx->mctx, listener);
	if (ret < 0)
	{
		TRACE_ERROR("Failed to set socket in nonblocking mode.\n");
		return -1;
	}

	saddr.sin_family = AF_INET;
	saddr.sin_addr.s_addr = INADDR_ANY;
	saddr.sin_port = htons(peer_port);
	ret = mtcp_bind(ctx->mctx, listener, (struct sockaddr *)&saddr, sizeof(struct sockaddr_in));
	if (ret < 0)
	{
		TRACE_ERROR("Failed to bind to the listening socket!\n");
		return -1;
	}

	/* listen (backlog: can be configured) */
	ret = mtcp_listen(ctx->mctx, listener, backlog);
	if (ret < 0)
	{
		TRACE_ERROR("mtcp_listen() failed!\n");
		return -1;
	}

	/* wait for incoming accept events */
	ev.events = MTCP_EPOLLIN;
	ev.data.sockid = listener;
	mtcp_epoll_ctl(ctx->mctx, ctx->ep, MTCP_EPOLL_CTL_ADD, listener, &ev);

	return listener;
}
/*----------------------------------------------------------------------------*/
static inline int CreateConnection(struct thread_context *ctx, char *peer_addr, int peer_port)
{
	mctx_t mctx = ctx->mctx;
	struct sockaddr_in addr;
	int sockid;
	int ret;
	struct mtcp_epoll_event ev;

	sockid = mtcp_socket(mctx, AF_INET, SOCK_STREAM, 0);
	if (sockid < 0)
	{
		TRACE_INFO("Failed to create socket!\n");
		return -1;
	}

	ret = mtcp_setsock_nonblock(mctx, sockid);
	if (ret < 0)
	{
		TRACE_ERROR("Failed to set socket in nonblocking mode.\n");
		exit(-1);
	}

	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = inet_addr(peer_addr);
	addr.sin_port = htons(peer_port);

	ret = mtcp_connect(mctx, sockid, (struct sockaddr *)&addr, sizeof(struct sockaddr_in));
	if (ret < 0)
	{
		if (errno != EINPROGRESS)
		{
			perror("mtcp_connect");
			mtcp_close(mctx, sockid);
			return -1;
		}
	}

	ev.events = MTCP_EPOLLOUT;
	ev.data.sockid = sockid;
	mtcp_epoll_ctl(mctx, ctx->ep, MTCP_EPOLL_CTL_ADD, sockid, &ev);

	{
		struct mtcp_epoll_event events[8];
		int found = 0;
		while (!found)
		{
			int i, nev = mtcp_epoll_wait(mctx, ctx->ep, events, 8, -1);
			for (i = 0; i < nev; i++)
			{
				if (events[i].data.sockid == sockid && (events[i].events & (MTCP_EPOLLOUT | MTCP_EPOLLERR)))
				{
					if (events[i].events & MTCP_EPOLLERR)
					{
						int err;
						socklen_t len = sizeof(err);
						mtcp_getsockopt(mctx, events[i].data.sockid, SOL_SOCKET, SO_ERROR, (void *)&err, &len);
						printf("Error on socket %d: %d (%s)\n", events[i].data.sockid, err, strerror(err));
					}
					found = 1;
					break;
				}
			}
		}
		ev.events = MTCP_EPOLLIN;
		ev.data.sockid = sockid;
		mtcp_epoll_ctl(mctx, ctx->ep, MTCP_EPOLL_CTL_MOD, sockid, &ev);
	}

	return sockid;
}
/*----------------------------------------------------------------------------*/
void *RunClientThread(void *arg)
{
	int core = *(int *)arg;
	struct thread_context *ctx;
	mctx_t mctx;
	int sockfd = 0;
	int ret;
	uint64_t sum_us = 0, count_us = 0;
	char* buf = malloc(ping_size);

	if (!buf)
	{
		TRACE_ERROR("Failed to allocate %d bytes.\n", ping_size);
		return NULL;
	}

	/* initialization */
	ctx = InitializeServerThread(core);
	if (!ctx)
	{
		TRACE_ERROR("Failed to initialize client thread.\n");
		return NULL;
	}
	mctx = ctx->mctx;

	sockfd = CreateConnection(ctx, peer_addr, peer_port);
	if (sockfd < 0)
	{
		goto end;
	}

	while (!done[core])
	{
		struct timespec start, end;
		clock_gettime(CLOCK_REALTIME, &start);
		memset(buf, 0xAA, ping_size);
		ret = write_poll(ctx, sockfd, buf, ping_size);
		if (ret < 0)
		{
			printf("write() failed - connection closed by server\n");
			break;
		}
/*		printf("Waiting\n");
		{
			struct mtcp_epoll_event events[8];
			int found = 0;
			while (!found)
			{
				int i, nev = mtcp_epoll_wait(mctx, ctx->ep, events, 8, -1);
				for (i = 0; i < nev; i++)
				{
					if (events[i].data.sockid == sockfd)
					{
						found = 1;
						break;
					}
				}
			}
		}*/
		ret = read_poll(ctx, sockfd, buf, ping_size);
		if (ret < 0)
		{
			printf("read() failed - connection closed by server\n");
			break;
		}
		clock_gettime(CLOCK_REALTIME, &end);
		sum_us += (end.tv_nsec - start.tv_nsec)/1000 + (end.tv_sec - start.tv_sec)*1000000;
		count_us++;
		printf("Done %lu us\n", (end.tv_nsec - start.tv_nsec)/1000 + (end.tv_sec - start.tv_sec)*1000000);
	}
	printf("Done\n");

end:
	printf("%lu messages received, %lu us avg ping-pong\n", count_us, sum_us/(count_us == 0 ? 1 : count_us));

	/* destroy mtcp context: this will kill the mtcp thread */
	if (sockfd)
		mtcp_close(ctx->mctx, sockfd);
	if (ctx)
		mtcp_destroy_context(mctx);
	if (buf)
		free(buf);
	pthread_exit(NULL);

	return NULL;
}
/*----------------------------------------------------------------------------*/
void *RunServerThread(void *arg)
{
	int core = *(int *)arg;
	struct thread_context *ctx;
	mctx_t mctx;
	int listener;
	int ep;
	struct mtcp_epoll_event *events;
	int nevents;
	int i, ret;
	int do_accept;
	
	/* initialization */
	ctx = InitializeServerThread(core);
	if (!ctx)
	{
		TRACE_ERROR("Failed to initialize server thread.\n");
		return NULL;
	}
	mctx = ctx->mctx;
	ep = ctx->ep;

	events = (struct mtcp_epoll_event *)calloc(MAX_EVENTS, sizeof(struct mtcp_epoll_event));
	if (!events)
	{
		TRACE_ERROR("Failed to create event struct!\n");
		exit(-1);
	}

	listener = CreateListeningSocket(ctx);
	if (listener < 0)
	{
		TRACE_ERROR("Failed to create listening socket.\n");
		exit(-1);
	}

	while (!done[core])
	{
		nevents = mtcp_epoll_wait(mctx, ep, events, MAX_EVENTS, -1);
		if (nevents < 0)
		{
			if (errno != EINTR)
				perror("mtcp_epoll_wait");
			break;
		}

		do_accept = FALSE;
		for (i = 0; i < nevents; i++)
		{
			if (events[i].data.sockid == listener)
			{
				/* if the event is for the listener, accept connection */
				do_accept = TRUE;
			}
			else if (events[i].events & MTCP_EPOLLERR)
			{
				int err;
				socklen_t len = sizeof(err);
				/* error on the connection */
				TRACE_APP("[CPU %d] Error on socket %d\n", core, events[i].data.sockid);
				if (mtcp_getsockopt(mctx, events[i].data.sockid, SOL_SOCKET, SO_ERROR, (void *)&err, &len) == 0)
				{
					if (err != ETIMEDOUT)
						fprintf(stderr, "Error on socket %d: %s\n", events[i].data.sockid, strerror(err));
				}
				else
				{
					perror("mtcp_getsockopt");
				}
				CloseConnection(ctx, events[i].data.sockid, &ctx->svars[events[i].data.sockid]);
			}
			else if (events[i].events & MTCP_EPOLLIN)
			{
				ret = HandleReadEvent(ctx, events[i].data.sockid, &ctx->svars[events[i].data.sockid]);
				if (ret < 0)
				{
					/* connection closed by remote host */
					CloseConnection(ctx, events[i].data.sockid, &ctx->svars[events[i].data.sockid]);
				}
			}
			else
			{
				assert(0);
			}
		}

		/* if do_accept flag is set, accept connections */
		if (do_accept)
		{
			while (1)
			{
				ret = AcceptConnection(ctx, listener);
				if (ret < 0)
					break;
			}
		}
	}

	/* destroy mtcp context: this will kill the mtcp thread */
	mtcp_destroy_context(mctx);
	pthread_exit(NULL);

	return NULL;
}
/*----------------------------------------------------------------------------*/
void SignalHandler(int signum)
{
	int i;
	for (i = 0; i < core_limit; i++)
	{
		if (app_thread[i] == pthread_self())
		{
			//TRACE_INFO("Server thread %d got SIGINT\n", i);
			done[i] = TRUE;
		}
		else if (app_thread[i])
		{
			if (!done[i])
			{
				pthread_kill(app_thread[i], signum);
			}
		}
	}
}
/*----------------------------------------------------------------------------*/
static void printHelp(const char *prog_name)
{
	TRACE_CONFIG("%s -f <mtcp_conf_file> [-c <peer_ip>] [-p <port>] [-s <ping_size>] [-h]\n", prog_name);
	exit(EXIT_SUCCESS);
}
/*----------------------------------------------------------------------------*/
int main(int argc, char **argv)
{
	int ret;
	int cores[MAX_CPUS];
	int process_cpu;
	int i, o;

	struct mtcp_conf mcfg;

	num_cores = GetNumCPUs();
	core_limit = num_cores;
	process_cpu = -1;

	while (-1 != (o = getopt(argc, argv, "f:c:p:s:h")))
	{
		switch (o)
		{
		case 'f':
			conf_file = optarg;
			break;
		case 'c':
			peer_addr = optarg;
			break;
		case 'p':
			peer_port = atoi(optarg);
			break;
		case 's':
			ping_size = atoi(optarg);
			break;
		case 'h':
			printHelp(argv[0]);
			break;
		}
	}

	if (peer_addr == NULL)
	{
		TRACE_CONFIG("Server mode\n");
	}

	/* initialize mtcp */
	if (conf_file == NULL)
	{
		TRACE_CONFIG("You forgot to pass the mTCP startup config file!\n");
		exit(EXIT_FAILURE);
	}

	ret = mtcp_init(conf_file);
	if (ret)
	{
		TRACE_CONFIG("Failed to initialize mtcp\n");
		exit(EXIT_FAILURE);
	}

	mtcp_getconf(&mcfg);
	if (backlog > mcfg.max_concurrency)
	{
		TRACE_CONFIG("backlog can not be set larger than CONFIG.max_concurrency\n");
		return FALSE;
	}

	/* if backlog is not specified, set it to 4K */
	if (backlog == -1)
	{
		backlog = 4096;
	}

	/* register signal handler to mtcp */
	mtcp_register_signal(SIGINT, SignalHandler);

	TRACE_INFO("Application initialization finished.\n");

	for (i = ((process_cpu == -1) ? 0 : process_cpu); i < core_limit; i++)
	{
		cores[i] = i;
		if (pthread_create(&app_thread[i], NULL, peer_addr == NULL ? RunServerThread : RunClientThread, (void *)&cores[i]))
		{
			perror("pthread_create");
			TRACE_CONFIG("Failed to create server thread.\n");
			exit(EXIT_FAILURE);
		}
		else
		{
			// Run on one core
			process_cpu = i;
			break;
		}
	}

	pthread_join(app_thread[process_cpu], NULL);

	mtcp_destroy();
	return 0;
}
