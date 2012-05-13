function console() {
	var _ = this;

	this.tmp_job_id =-1;

	function do_updates() {
		update();
		setTimeout( do_updates, 1500 )
	}

	function node_id(n) {
		return (n.host + "-" + n.port).replace(/\./g, '_');
	} 

	function job_id(j) {
		return "job_" + j.id;
	}

	function node_name(n) {
		return n.host + ":" + n.port;
	} 


	function render_nodes(updates) {
		$("#nodes li").addClass("remove");

		for (var i = 0; i < updates.length; i++) {
			// Check to see if we already have a node here?
			var n = updates[i];


			var li = $("#" + node_id(n) );
			if ( li.length==0){
				// Create a new list item for you please!
				li = $("#node-template").clone();
				li.attr("id", node_id(n));
				$("#nodes").append( li );
			}
			li.removeClass("remove");
			li.find(".node-name").text(node_name(n));
			li.find(".cpu-value").text( parseInt(n.cpu) + "%" );
			li.find(".job-value").text( n.current_job );
		}
		$("#nodes li.remove").remove()
	}

	function render_jobs(updates) {
		$("#jobs li").addClass("remove");
		for (var i = 0; i < updates.length; i++) {
			// Check to see if we already have a node here?
			var j = updates[i];


			var li = $("#" + job_id(j) );
			if ( li.length==0){
				// Create a new list item for you please!
				li = $("#job-template").clone();
				li.attr("id", job_id(j));
				$("#jobs").append( li );
			}

			li.removeClass("remove");
			li.find(".job-name").text(j.name + " [" + j.id + "]");
			li.find(".job-status").text( j.status );
			li.find(".job-command").text( j.executable );
			li.find(".job-queued").text( j.work_count_queued + " / " + j.work_count_active + " / " + j.work_count_complete);
		}
		$("#jobs li.remove").remove()
	}

	function update() {
		$.ajax({
			url: "/json/nodes",
			success: function (response) {
				render_nodes(response.nodes);
			},
			error: function () {
			}
		});


		$.ajax({
			url: "/json/jobs",
			success: function (response) {
				render_jobs(response.jobs);
			},
			error: function () {
			}
		});
	}

	do_updates();

	// Stuff for handling job submission

	$("#text-job-name,#text-job-cmd,").blur( function() {
		var txt = $("#text-job-name");
		var cmd = $("#text-job-cmd");

		if (cmd.val().indexOf("/") > -1){
			alert("Your command may not include a '/' character")
		}
		if (txt.val().indexOf("/") > -1){
			alert("Your name may not include a '/' character")
		}

		if (txt.val().length > 0) {
			update_job(txt.val(), cmd.val())

		}
	});

	$("#enqueue").click(function() {

		$.ajax({
			url: "/json/job/queue/",
			type: "POST",
			data: {tmp_job_id: _.tmp_job_id},
			success: function (response) {

				// Reset the form and hide everything that needs hiding
				_.tmp_job_id = -1
				$("#text-job-name").val("");
				$("#text-job-cmd").val("");
				$(".qq-upload-list").empty();
				$(".files").css("display", "none")
				$("#enqueue").css("display", "none");

				// alert("Your job has been queued with JobId:" + response.job_id)
			},
			error: function () {
			}
		});

	});

	function update_job(name, command){

		$.ajax({
			url: "/json/job/update/",
			type: "POST",
			data: {tmp_job_id: _.tmp_job_id, name: name, command: command},
			success: function (response) {
				if (_.tmp_job_id == -1){
					_.tmp_job_id = response.tmp_job_id;
					create_uploader(_.tmp_job_id);
					$(".files").css("display", "")
				}
			},
			error: function () {
			}
		});

	}


	function create_uploader(tmpid){
		var uploader = new qq.FileUploader({
			element: document.getElementById("upload"),
			action: '/json/job/submit-file/'+tmpid+"/",
	//		allowedExtensions: ['jpg', 'jpeg', 'png', 'gif'],
			onComplete: function (id, fileName, responseJSON) {
				$("#enqueue").css("display", "block");

				return true;
			},
			onSubmit: function (id, fileName) {
				// $("ul.qq-upload-list").addClass("visible")
				return true;
			},

			onFileDraggedOver: function (isActive) {
				$("#drop-target").css("display", "block");
			},
			onCancel: function () {
				alert("canceled");
			}

		});

	}



}
