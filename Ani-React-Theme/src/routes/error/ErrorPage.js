

import React, { PropTypes } from 'react';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import s from './ErrorPage.css';
import Link from '../../components/Link';

import profilePage from '../../common/images/flat-avatar.png';

function ErrorPage({ error }, context) {
  let title = 'Error';
  let content = 'Sorry, a critical error occurred on this page.';
  let errorMessage = null;
  let errorCode = 404;

  if (error.status === 404) {
    title = 'Page Not Found';
    content = 'Sorry, the page you were trying to view does not exist.';
    errorCode = error.status;
  } else if (process.env.NODE_ENV !== 'production') {
    errorMessage = <p>{error.stack}</p>;
  }

  if (context.setTitle) {
    context.setTitle(title);
  }

  return (
    <div className={` animate ${s.ErrorPage}`}>
      <div className="row">
        <div className="col-md-4 col-lg-4 col-md-offset-4 col-lg-offset-4">
          <Link to="/" className="hvr-pulse-grow">
            <img src={profilePage} className="user-avatar" alt="profile" />
          </Link>
          <h1>Ani Theme </h1>
          <div>
            <h1>{errorCode}</h1>
            <p>{content}</p>
            <p>{errorMessage}</p>
          </div>
        </div>
      </div>
    </div>
  );
}

ErrorPage.propTypes = { error: PropTypes.object.isRequired };
ErrorPage.contextTypes = { setTitle: PropTypes.func.isRequired };

export { ErrorPage as ErrorPageWithoutStyle };
export default withStyles(s)(ErrorPage);
