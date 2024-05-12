/**
 * React Starter Kit (https://www.reactstarterkit.com/)
 *
 * Copyright © 2014-2016 Kriasoft, LLC. All rights reserved.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE.txt file in the root directory of this source tree.
 */

import React, { PropTypes, Component } from 'react';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import { Button, FormGroup, FormControl } from 'react-bootstrap';
import Link from '../../components/Link';
import s from './Login.css';
import flatAvatar from '../../common/images/flat-avatar.png';
import History from '../../core/history';

const title = 'Log In';


class Login extends Component {
  static contextTypes = {
    setTitle: PropTypes.func.isRequired,
  };

  constructor(props) {
    super(props);
    this.state = {
      loginID: '',
      password: '',
      isSubmitted: false,
    };
    this.handleLogin = this.handleLogin.bind(this);
  }

  componentWillMount() {
    this.context.setTitle(title);
  }
  componentDidMount() {
    [].slice.call(document.querySelectorAll('button.progress-login'))
    .forEach((bttn) => {
      new ProgressButton(bttn, { // eslint-disable-line
        callback: (instance) => {
          let progress = 0;
          const interval = setInterval(() => {
            progress = Math.min(progress + (Math.random() * 0.1), 1);
            instance._setProgress(progress); // eslint-disable-line

            if (progress === 1) {
              instance._stop(1); // eslint-disable-line
              clearInterval(interval);
              setTimeout(() => {
                History.push('/');
              }, 500);
            }
          }, 200);
        },
      });
    });
    // [].slice.call(document.querySelectorAll('button.progress-button')).forEach((bttn) => {
    //   new ProgressButton(bttn, { //eslint-disable-line
    //     callback: (instance) => {
    //       let progress = 0;
    //       const interval = setInterval(() => {
    //         progress = Math.min(progress + (Math.random() * 0.1), 1);
    //         instance._setProgress(progress); //eslint-disable-line
    //         if (progress === 1) {
    //           instance._stop(1); // eslint-disable-line
    //           clearInterval(interval);
    //           History.push('/');
    //         }
    //       }, 200);
    //     },
    //   });
    // });
  }

  handleLogin(e) {
    e.preventDefault();
    return false;
  }

  render() {
    return (
      <div className={`login-page animate ${s.loginPage}`}>
        <div className="row">
          <div className="col-md-4 col-lg-4 col-md-offset-4 col-lg-offset-4">
            <img src={flatAvatar} alt="flat Avatar" className="user-avatar" />
            <h1>Ani Theme </h1>
            <form role="form" onSubmit={this.handleLogin}>
              <div className={s.formContent}>
                <FormGroup>
                  <FormControl
                    id=""
                    type="text"
                    placeholder="Email"
                    className={s.inputStyle}
                  />
                </FormGroup>
                <FormGroup>
                  <FormControl
                    id=""
                    type="text"
                    placeholder="Password"
                    className={s.inputStyle}
                  />
                </FormGroup>
              </div>

              <button
                type="submit"
                className={`btn btn-white btn-outline btn-lg btn-rounded progress-login
                  progress-button ${s.btn}`}
                data-style="fill" data-horizontal
              >Log in</button>
              <span>&nbsp;&nbsp;</span>
              <Link to="/signup">
                <Button
                  type="submit"
                  className={
                    `btn btn-white btn-outline btn-lg btn-rounded ${s.btn}`
                  }
                >Register
                </Button>
              </Link>
            </form>
          </div>
        </div>
      </div>
    );
  }
}


Login.contextTypes = { setTitle: PropTypes.func.isRequired };

export default withStyles(s)(Login);
