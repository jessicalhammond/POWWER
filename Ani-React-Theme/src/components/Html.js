import React, { PropTypes } from 'react';
import { analytics } from '../config';

function Html({ title, description, style, script, children }) {
  return (
    <html className="no-js" lang="en">
      <head>
        <meta charSet="utf-8" />
        <meta httpEquiv="x-ua-compatible" content="ie=edge" />
        <title>{title}</title>
        <meta name="description" content={description} />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="stylesheet" href="http://fonts.googleapis.com/css?family=Open+Sans:300italic,400italic,600italic,400,300,600" />
        <link rel="apple-touch-icon" href="apple-touch-icon.png" />
        <link rel="stylesheet" href="/css/font-awesome.min.css" />
        <link rel="stylesheet" href="/css/animate.min.css" />
        <link rel="stylesheet" href="/css/hover-min.css" />
        <link rel="stylesheet" href="/css/nprogress.css" />
        <link rel="stylesheet" href="/css/rc-calendar/index.css" />
        <link rel="stylesheet" href="/css/rc-select/index.css" />
        <link rel="stylesheet" href="/css/gemini-scrollbar.css" />
        <link rel="stylesheet" href="/css/component.css" />
        <link rel="stylesheet" href="/css/react-datepicker.css" />
        <link
          href="//maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css"
          type="text/css"
          rel="stylesheet"
        />
        <script src="/js/modernizr.custom.js" />

        {/*
        <script src="/moment/moment.js" />
        <link rel="stylesheet" href="/fullcalendar/fullcalendar.css" />
        <script src="/fullcalendar/fullcalendar.js" />
        */}

        <style id="css" dangerouslySetInnerHTML={{ __html: style }} />
      </head>
      <body>
        <div id="app" dangerouslySetInnerHTML={{ __html: children }} />
        {script && <script src={script} />}
        {analytics.google.trackingId &&
          <script
            dangerouslySetInnerHTML={{ __html:
            'window.ga=function(){ga.q.push(arguments)};ga.q=[];ga.l=+new Date;' +
            `ga('create','${analytics.google.trackingId}','auto');ga('send','pageview')` }}
          />
        }
        {analytics.google.trackingId &&
          <script src="https://www.google-analytics.com/analytics.js" async defer />
        }
        <script src="/js/classie.js" />
        <script src="/js/progressButton.js" />
      </body>
    </html>
  );
}

Html.propTypes = {
  title: PropTypes.string.isRequired,
  description: PropTypes.string.isRequired,
  style: PropTypes.string.isRequired,
  script: PropTypes.string,
  children: PropTypes.string,
};

export default Html;
