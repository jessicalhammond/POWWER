import React, { Component, PropTypes } from 'react';
import { Button, FormGroup, FormControl } from 'react-bootstrap';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import s from './SignUp.css';
// import withStyles from 'isomorphic-style-loader/lib/withStyles';
import Link from '../../components/Link';
import History from '../../core/history';
import flatAvatar from '../../common/images/flat-avatar.png';

const title = 'Sign Up';

class SignUp extends Component {

  static contextTypes = {
    setTitle: PropTypes.func.isRequired,
  };
  constructor(props) {
    super(props);
    this.register = this.register.bind(this);
  }


  componentWillMount() {
    this.context.setTitle(title);
  }
  register(e) {
    e.preventDefault();
    History.push('/');
    return false;
  }
  render() {
    return (
      <div className={`animate ${s.signUp}`}>
        <div className="row">
          <div className="col-md-4 col-lg-4 col-md-offset-4 col-lg-offset-4">
            <img src={flatAvatar} alt="flat Avatar" className="user-avatar" />
            <h1>Ani Theme </h1>
            <form role="form" onSubmit={this.register}>
              <div className={s.formContent}>
                <FormGroup>
                  <FormControl
                    id=""
                    type="text"
                    placeholder="Full Name"
                    className={s.inputStyle}
                  />
                </FormGroup>
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
                <FormGroup>
                  <FormControl
                    id=""
                    type="text"
                    placeholder="Repeat Password"
                    className={s.inputStyle}
                  />
                </FormGroup>
              </div>
              <Button
                type="submit"
                className={
                    `btn btn-white btn-outline btn-lg btn-rounded progress-login ${s.btn}`
                  }
              >
                Register
              </Button>
              <span>&nbsp;&nbsp;</span>
              <Link to="/login">
                <Button
                  type="submit"
                  className={
                    `btn btn-white btn-outline btn-lg btn-rounded progress-login ${s.btn}`
                  }
                >Log in
                </Button>
              </Link>

            </form>
          </div>
        </div>
      </div>
    );
  }
}

export default withStyles(s)(SignUp);
